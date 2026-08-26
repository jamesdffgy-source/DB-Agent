#!/usr/bin/env python3
"""DB-Agent database operation core.

Natural-language requests are mapped to a typed operation plan and then routed
through schema, semantic, authorization, and execution boundaries:
  - 要"看结构" → 本地元数据查看（无需 LLM）
  - 要"算"      → NL2SQL 查数（COUNT/AVG/JOIN/...）
  - 要"描述"    → RAG 检索问答（召回 + 组织）
  - 要"组合推理" → 工具编排（查数 + 检索 + 规则，多步）
  - 要"改数据"  → 生成写操作预览，用户确认后执行

The module owns database planning and enforcement. Desktop transport, model
profiles, and the purpose-built model gateway are separate components.

安全：查询 SQL 在只读连接中执行；写 SQL 必须先生成预览并经一次性确认单批准后执行。

模块结构：
  DBConnector      —— 只读 sqlite 连接管理（安全连接工厂）
  SchemaDiscovery  —— 读 sqlite_master → 表/列/类型/主外键/行数 → schema 快照 + L1 索引
  SQLSecurity      —— 单语句校验/写操作拦截/LIMIT 强制/超时
  NL2SQLExecutor   —— LLM 生成 SQL → 安全执行 → 表格化 → 自纠错（最多2轮）
  RagRetriever     —— 表/列语义 + 值域 → 关键词/FTS 召回 → LLM 组织自然语言回答
  IntentRouter     —— LLM 判断三意图 + 置信度
  OperationGraph   —— 自研只读 DAG（依赖校验、拓扑执行、部分失败降级）
  DBAgent          —— 总入口：一问一答，路由到对应执行器
"""
from __future__ import annotations

import calendar as pycalendar
import contextvars
import json
import math
import os
import re
import socket
import sqlite3
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import timezone_release_contract as timezone_contract


try:
    _TIMEZONE_MANIFEST = timezone_contract.load_manifest()
    _TIMEZONE_CONTRACT_ERROR: Optional[str] = None
except (OSError, ValueError) as exc:
    _TIMEZONE_MANIFEST = None
    _TIMEZONE_CONTRACT_ERROR = str(exc)


class TimezoneRuntime:
    """项目自带、可并存和可回滚的 IANA 运行时。"""

    _MANIFEST = _TIMEZONE_MANIFEST
    _ACTIVE_RELEASE_ID = (
        str(_MANIFEST["active_release_id"]) if _MANIFEST is not None else ""
    )
    _ACTIVE_RELEASE = (
        dict(_MANIFEST["releases"][_ACTIVE_RELEASE_ID]) if _MANIFEST is not None else {}
    )
    TZDATA_VERSION = str(_ACTIVE_RELEASE.get("tzdata_version") or "")
    IANA_VERSION = str(_ACTIVE_RELEASE.get("iana_version") or "")
    VERSION_TOKEN = f"tzdata-{TZDATA_VERSION}/iana-{IANA_VERSION}"
    SQL_FUNCTION = "dbagent_iana_date"

    @staticmethod
    @lru_cache(maxsize=128)
    def _zone(release_id: str, name: str) -> ZoneInfo:
        if TimezoneRuntime._MANIFEST is None:
            raise ValueError(_TIMEZONE_CONTRACT_ERROR or "时区发布清单不可用")
        return timezone_contract.load_zone(TimezoneRuntime._MANIFEST, release_id, name)

    @classmethod
    def status(cls) -> dict:
        if cls._MANIFEST is None:
            return {
                "available": False,
                "release_id": None,
                "tzdata_version": None,
                "iana_version": None,
                "version_token": None,
                "archive_sha256": None,
                "zones_count": 0,
                "release_count": 0,
                "rollback_release_id": None,
                "error": _TIMEZONE_CONTRACT_ERROR or "时区发布清单不可用",
            }
        try:
            report = timezone_contract.validate_release_archive(
                cls._MANIFEST, cls._ACTIVE_RELEASE_ID, run_probes=False,
            )
        except (OSError, ValueError) as exc:
            return {
                "available": False,
                "release_id": cls._ACTIVE_RELEASE_ID or None,
                "tzdata_version": None,
                "iana_version": None,
                "version_token": None,
                "archive_sha256": None,
                "zones_count": 0,
                "release_count": len(cls._MANIFEST["releases"]),
                "rollback_release_id": cls._MANIFEST.get("rollback_release_id"),
                "error": str(exc),
            }
        return {
            "available": True,
            "release_id": report["release_id"],
            "tzdata_version": report["tzdata_version"],
            "iana_version": report["iana_version"],
            "version_token": report["version_token"],
            "archive_sha256": report["sha256"],
            "zones_count": report["zones_count"],
            "release_count": len(cls._MANIFEST["releases"]),
            "rollback_release_id": cls._MANIFEST.get("rollback_release_id"),
            "error": None,
        }

    @classmethod
    def validate_contract(cls) -> dict:
        return timezone_contract.validate_contract(run_probes=True)

    @classmethod
    def resolve_release(
        cls,
        tzdata_version: Optional[str] = None,
        iana_version: Optional[str] = None,
    ) -> tuple[str, dict]:
        if cls._MANIFEST is None:
            raise ValueError(_TIMEZONE_CONTRACT_ERROR or "时区发布清单不可用")
        tzdata = str(tzdata_version or "").strip()
        iana = str(iana_version or "").strip()
        if not tzdata and not iana:
            return cls._ACTIVE_RELEASE_ID, dict(cls._ACTIVE_RELEASE)
        if not tzdata or not iana:
            raise ValueError("业务日历必须同时提供 tzdata_version 和 iana_version")
        matched = timezone_contract.find_release(cls._MANIFEST, tzdata, iana)
        if matched is None:
            raise ValueError(
                "业务日历 tzdata_version/iana_version 引用未归档的时区版本: "
                f"{tzdata}/IANA {iana}"
            )
        return matched[0], dict(matched[1])

    @classmethod
    def version_token_for(cls, tzdata_version: str, iana_version: str) -> str:
        _, release = cls.resolve_release(tzdata_version, iana_version)
        return timezone_contract.version_token(release)

    @classmethod
    def require_zone(
        cls,
        name: str,
        *,
        tzdata_version: Optional[str] = None,
        iana_version: Optional[str] = None,
    ) -> ZoneInfo:
        status = cls.status()
        if not status["available"]:
            raise ValueError(str(status["error"]))
        release_id, _ = cls.resolve_release(tzdata_version, iana_version)
        try:
            return cls._zone(release_id, str(name))
        except (OSError, ValueError) as exc:
            raise ValueError(f"业务日历 IANA 时区不存在: {name}") from exc

    @classmethod
    def utc_to_local_date(cls, value: Any, zone_name: Any, version_token: Any) -> Optional[str]:
        """SQLite UDF：严格接收 UTC ISO-8601 值，按固定数据版本返回业务日期。"""
        if value is None or cls._MANIFEST is None:
            return None
        matched = timezone_contract.release_for_token(cls._MANIFEST, str(version_token))
        if matched is None:
            return None
        release_id, _ = matched
        raw = str(value).strip()
        if not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?"
            r"(?:[Zz]|[+-]\d{2}:\d{2})?",
            raw,
        ):
            return None
        normalized = raw[:-1] + "+00:00" if raw.endswith(("Z", "z")) else raw
        try:
            parsed = datetime.fromisoformat(normalized)
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        elif parsed.utcoffset() != timedelta(0):
            # storage_basis=utc_datetime 不接受带非零偏移的源值，避免双重换算。
            return None
        try:
            zone = cls._zone(release_id, str(zone_name))
        except (OSError, ValueError):
            return None
        return parsed.astimezone(zone).date().isoformat()

    @classmethod
    def register_sqlite(cls, conn: sqlite3.Connection) -> None:
        status = cls.status()
        if not status["available"]:
            return
        conn.create_function(
            cls.SQL_FUNCTION,
            3,
            cls.utc_to_local_date,
            deterministic=True,
        )

# ---------------------------------------------------------------------------
# 数据类（统一的数据载体，供 bridge / 前端 / 各执行器共享）
# ---------------------------------------------------------------------------

@dataclass
class DBColumn:
    """列元信息。"""
    name: str
    type: str
    nullable: bool = True
    pk: bool = False
    fk_table: Optional[str] = None      # 外键指向表
    fk_column: Optional[str] = None     # 外键指向列
    sample_values: List[Any] = field(default_factory=list)  # 值域抽样（用于 RAG 词表）
    semantic_name: str = ""             # 可选业务名称（例如公开数据字典中的列别名）
    description: str = ""               # 可选列定义；只在与当前问题相关时进入模型上下文
    value_description: str = ""         # 可选值域说明；只截取与当前问题相关的片段
    default_sql: str = ""               # SQLite schema default expression; empty means no default


def is_time_column(column: DBColumn) -> bool:
    """统一识别可用于时间分析的字段，供语义校验和歧义门禁复用。"""
    return bool(
        re.search(r"(DATE|TIME|YEAR)", column.type or "", re.IGNORECASE)
        or re.search(
            r"(^|_)(date|time|timestamp|created|updated|year|month|day)(_|$)|日期|时间|年|月|日",
            column.name,
            re.IGNORECASE,
        )
    )


@dataclass
class DBTable:
    """表元信息。"""
    name: str
    columns: List[DBColumn] = field(default_factory=list)
    row_count: int = 0
    create_sql: str = ""


@dataclass
class SchemaSnapshot:
    """schema 快照：紧凑版供 LLM 上下文；完整版供程序使用。"""
    db_path: str
    tables: Dict[str, DBTable] = field(default_factory=dict)
    generated_at: float = 0.0

    def compact(self, max_cols_per_table: int = 12) -> str:
        """Build a bounded schema summary for model context."""
        lines = []
        for tname, tbl in self.tables.items():
            cols = []
            for c in tbl.columns[:max_cols_per_table]:
                tag = "PK" if c.pk else (f"FK->{c.fk_table}.{c.fk_column}" if c.fk_table else "")
                cols.append(f"{c.name}:{c.type}" + (f"({tag})" if tag != "" else ""))
            lines.append(f"TABLE {tname} ({tbl.row_count} rows) | " + ", ".join(cols))
        return "\n".join(lines)

    @staticmethod
    def _singular_identifier_token(token: str) -> str:
        if token in {"series", "species"} or not token.isascii():
            return ""
        if len(token) > 4 and token.endswith("ies"):
            return token[:-3] + "y"
        if len(token) > 4 and token.endswith(
            ("sses", "xes", "zes", "ches", "shes")
        ):
            return token[:-2]
        if len(token) > 3 and token.endswith("s") \
                and not token.endswith(("ss", "us", "is")):
            return token[:-1]
        return ""

    @classmethod
    def _identifier_tokens(cls, value: str) -> set[str]:
        expanded = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(value or ""))
        tokens: set[str] = set()
        for raw in re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", expanded):
            token = raw.casefold()
            if not token:
                continue
            tokens.add(token)
            # Schema linking needs a small amount of deterministic morphology:
            # natural questions commonly use plural business nouns while
            # physical identifiers use singular tokens.  Retain both forms
            # and avoid words whose terminal ``s`` is not a plural marker.
            singular = cls._singular_identifier_token(token)
            if singular:
                tokens.add(singular)
        return tokens

    @classmethod
    def _canonical_identifier_tokens(cls, value: str) -> set[str]:
        expanded = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(value or ""))
        tokens: set[str] = set()
        for raw in re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", expanded):
            token = raw.casefold()
            if token:
                tokens.add(cls._singular_identifier_token(token) or token)
        return tokens

    @classmethod
    def _canonical_tokens_from_identifier_tokens(
        cls, tokens: set[str] | frozenset[str],
    ) -> set[str]:
        return {
            cls._singular_identifier_token(token) or token
            for token in tokens
        }

    @classmethod
    def _meaningful_tokens_from_identifier_tokens(
        cls, tokens: set[str] | frozenset[str],
    ) -> set[str]:
        return {
            token for token in tokens
            if token not in cls._DESCRIPTION_STOPWORDS
            and (len(token) >= 2 or token.isdigit())
        }

    @staticmethod
    def _column_text(column: DBColumn) -> str:
        tag = (
            "PK" if column.pk
            else f"FK->{column.fk_table}.{column.fk_column}" if column.fk_table
            else ""
        )
        return f"{column.name}:{column.type}" + (f"({tag})" if tag else "")

    def _schema_context_index_signature(self) -> tuple[Any, ...]:
        """Return a cheap structural signature for the prepared context index.

        ``SchemaSnapshot`` remains intentionally mutable because discovery and
        access scoping assemble it incrementally.  A cache keyed only by object
        identity would therefore serve stale table/column/FK/dictionary facts.
        The signature retains references to the immutable field values, so an
        unchanged snapshot compares cheaply while in-place schema edits force
        a rebuild before another question is rendered.
        """
        return tuple(
            (
                table_name,
                tuple(
                    (
                        column.name,
                        bool(column.pk),
                        str(column.fk_table or ""),
                        str(column.fk_column or ""),
                        str(column.semantic_name or ""),
                        str(column.description or ""),
                        str(column.value_description or ""),
                    )
                    for column in table.columns
                ),
            )
            for table_name, table in self.tables.items()
        )

    def _prepared_schema_context_index(self) -> Dict[str, Any]:
        """Compile reusable schema-linking tokens, frequencies and FK graph."""
        signature = self._schema_context_index_signature()
        cached = getattr(self, "_schema_context_index_cache", None)
        if cached is not None and cached[0] == signature:
            return cached[1]

        identifier_analysis_cache: Dict[
            str, tuple[frozenset[str], frozenset[str], frozenset[str], str]
        ] = {}

        def analyze_identifier(
            value: str,
        ) -> tuple[frozenset[str], frozenset[str], frozenset[str], str]:
            key = str(value or "")
            analyzed = identifier_analysis_cache.get(key)
            if analyzed is not None:
                return analyzed
            tokens = frozenset(self._identifier_tokens(key))
            analyzed = (
                tokens,
                frozenset(self._canonical_tokens_from_identifier_tokens(tokens)),
                frozenset(self._meaningful_tokens_from_identifier_tokens(tokens)),
                re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", key.casefold()),
            )
            identifier_analysis_cache[key] = analyzed
            return analyzed

        table_tokens_by_name = {}
        table_canonical_tokens_by_name = {}
        table_meaningful_tokens_by_name = {}
        table_compact_by_name = {}
        for table_name in self.tables:
            tokens, canonical_tokens, meaningful_tokens, compact = (
                analyze_identifier(table_name)
            )
            table_tokens_by_name[table_name] = tokens
            table_canonical_tokens_by_name[table_name] = canonical_tokens
            table_meaningful_tokens_by_name[table_name] = meaningful_tokens
            table_compact_by_name[table_name] = compact
        table_token_frequency: Dict[str, int] = {}
        for tokens in table_tokens_by_name.values():
            for token in tokens:
                table_token_frequency[token] = table_token_frequency.get(token, 0) + 1
        table_rare_limit = max(2, (len(self.tables) + 19) // 20)
        distinctive_table_tokens_by_name = {
            table_name: frozenset(
                token for token in tokens
                if table_token_frequency.get(token, 0) <= table_rare_limit
                and (len(token) >= 2 or token.isdigit())
            )
            for table_name, tokens in table_tokens_by_name.items()
        }

        column_tokens_by_position: Dict[tuple[str, int], frozenset[str]] = {}
        column_canonical_tokens_by_position: Dict[
            tuple[str, int], frozenset[str]
        ] = {}
        column_compact_by_position: Dict[tuple[str, int], str] = {}
        column_metadata_tokens_by_position: Dict[
            tuple[str, int], frozenset[str]
        ] = {}
        column_dictionary_tokens_by_position: Dict[
            tuple[str, int],
            tuple[frozenset[str], frozenset[str], frozenset[str]],
        ] = {}
        meaningful_value_cache: Dict[str, frozenset[str]] = {"": frozenset()}

        def analyze_meaningful_value(value: str) -> frozenset[str]:
            key = str(value or "").strip()
            cached_tokens = meaningful_value_cache.get(key)
            if cached_tokens is not None:
                return cached_tokens
            cached_tokens = frozenset(self._meaningful_tokens(key))
            meaningful_value_cache[key] = cached_tokens
            return cached_tokens

        column_token_frequency: Dict[str, int] = {}
        total_columns = 0
        for table_name, table in self.tables.items():
            for column_order, column in enumerate(table.columns):
                total_columns += 1
                position = (table_name, column_order)
                tokens, canonical_tokens, _meaningful_tokens, compact = (
                    analyze_identifier(column.name)
                )
                column_tokens_by_position[position] = tokens
                column_canonical_tokens_by_position[position] = canonical_tokens
                column_compact_by_position[position] = compact
                semantic_tokens = analyze_meaningful_value(column.semantic_name)
                description_tokens = analyze_meaningful_value(column.description)
                value_tokens = analyze_meaningful_value(column.value_description)
                metadata_tokens = semantic_tokens | description_tokens | value_tokens
                if metadata_tokens:
                    column_dictionary_tokens_by_position[position] = (
                        semantic_tokens, description_tokens, value_tokens,
                    )
                    column_metadata_tokens_by_position[position] = metadata_tokens
                for token in tokens:
                    column_token_frequency[token] = (
                        column_token_frequency.get(token, 0) + 1
                    )
        column_rare_limit = max(4, (total_columns + 99) // 100)
        distinctive_tokens_cache: Dict[frozenset[str], frozenset[str]] = {}
        distinctive_column_tokens_by_position = {}
        for position, tokens in column_tokens_by_position.items():
            distinctive_tokens = distinctive_tokens_cache.get(tokens)
            if distinctive_tokens is None:
                distinctive_tokens = frozenset(
                    token for token in tokens
                    if column_token_frequency.get(token, 0) <= column_rare_limit
                    and (len(token) >= 2 or token.isdigit())
                )
                distinctive_tokens_cache[tokens] = distinctive_tokens
            distinctive_column_tokens_by_position[position] = distinctive_tokens

        prepared = {
            "total_columns": total_columns,
            "table_tokens": table_tokens_by_name,
            "table_canonical_tokens": table_canonical_tokens_by_name,
            "table_meaningful_tokens": table_meaningful_tokens_by_name,
            "table_compact": table_compact_by_name,
            "distinctive_table_tokens": distinctive_table_tokens_by_name,
            "column_tokens": column_tokens_by_position,
            "column_canonical_tokens": column_canonical_tokens_by_position,
            "column_compact": column_compact_by_position,
            "column_metadata_tokens": column_metadata_tokens_by_position,
            "column_dictionary_tokens": column_dictionary_tokens_by_position,
            "distinctive_column_tokens": distinctive_column_tokens_by_position,
        }
        setattr(self, "_schema_context_index_cache", (signature, prepared))
        return prepared

    def _prepared_fk_adjacency(
        self,
        prepared_index: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, tuple[Any, ...]]:
        """Build the declared FK graph lazily and reuse it per schema signature."""
        context_index = prepared_index or self._prepared_schema_context_index()
        cached = context_index.get("fk_adjacency")
        if cached is not None:
            return cached
        canonical_tables = {name.casefold(): name for name in self.tables}
        adjacency: Dict[
            str, List[tuple[str, tuple[tuple[str, int], ...]]]
        ] = {name: [] for name in self.tables}
        for source_name, table in self.tables.items():
            for source_index, column in enumerate(table.columns):
                target_name = canonical_tables.get(
                    str(column.fk_table or "").casefold(),
                )
                if not target_name or not column.fk_column:
                    continue
                target_indexes = [
                    index for index, target_column
                    in enumerate(self.tables[target_name].columns)
                    if target_column.name.casefold() == column.fk_column.casefold()
                ]
                if len(target_indexes) != 1:
                    continue
                edge_columns = (
                    (source_name, source_index),
                    (target_name, target_indexes[0]),
                )
                adjacency[source_name].append((target_name, edge_columns))
                adjacency[target_name].append((source_name, edge_columns))
        frozen_adjacency = {}
        for table_name, edges in adjacency.items():
            edges.sort(key=lambda item: (item[0].casefold(), item[1]))
            frozen_adjacency[table_name] = tuple(edges)
        context_index["fk_adjacency"] = frozen_adjacency
        return frozen_adjacency

    def _unique_shortest_fk_path_columns(
        self,
        start: str,
        goal: str,
        *,
        max_hops: int = 8,
        adjacency: Optional[Dict[str, tuple[Any, ...]]] = None,
    ) -> List[tuple[str, int]]:
        """Return columns for one uniquely proven undirected FK path.

        Large-schema prompts cannot afford every relationship column.  When
        two question-matched tables have exactly one shortest declared path,
        reserve both physical columns of every edge.  Parallel/equal paths and
        paths beyond the bounded hop count deliberately return no authority.
        """
        if start == goal or max_hops < 1:
            return []
        if start not in self.tables or goal not in self.tables:
            return []
        effective_adjacency = (
            adjacency if adjacency is not None else self._prepared_fk_adjacency()
        )

        distance = {start: 0}
        path_count = {start: 1}
        previous: Dict[
            str, tuple[str, tuple[tuple[str, int], ...]]
        ] = {}
        queue = [start]
        cursor = 0
        while cursor < len(queue):
            current = queue[cursor]
            cursor += 1
            current_distance = distance[current]
            if current_distance >= max_hops:
                continue
            for neighbor, edge_columns in effective_adjacency[current]:
                candidate_distance = current_distance + 1
                if neighbor not in distance:
                    distance[neighbor] = candidate_distance
                    path_count[neighbor] = min(2, path_count[current])
                    previous[neighbor] = (current, edge_columns)
                    queue.append(neighbor)
                elif distance[neighbor] == candidate_distance:
                    path_count[neighbor] = min(
                        2, path_count[neighbor] + path_count[current],
                    )
        if goal not in distance or path_count.get(goal) != 1:
            return []
        columns: List[tuple[str, int]] = []
        current = goal
        while current != start:
            prior = previous.get(current)
            if prior is None:
                return []
            parent, edge_columns = prior
            for item in edge_columns:
                if item not in columns:
                    columns.append(item)
            current = parent
        return columns

    _DESCRIPTION_STOPWORDS = {
        "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
        "has", "have", "in", "is", "it", "of", "on", "or", "that", "the",
        "this", "to", "was", "were", "what", "which", "who", "with",
    }

    @classmethod
    def _meaningful_tokens(cls, value: str) -> set[str]:
        return cls._meaningful_tokens_from_identifier_tokens(
            cls._identifier_tokens(value),
        )

    @staticmethod
    def _clean_description(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _relevant_description_excerpt(
        cls,
        value: str,
        question_tokens: set[str],
        max_chars: int,
    ) -> str:
        """Return bounded dictionary fragments that overlap the current question.

        Public/business data dictionaries can be very large and may contain
        hundreds of unrelated enum definitions.  Sending the whole dictionary
        both wastes context and increases schema-linking noise.  Keep only the
        best matching sentence/bullet fragments and never exceed the local cap.
        """
        raw = str(value or "").strip()
        if not raw or max_chars < 1:
            return ""
        fragments = [
            cls._clean_description(item)
            for item in re.split(r"[\r\n]+|(?<=[.!?])\s+", raw)
            if cls._clean_description(item)
        ]
        ranked: List[tuple[int, int, str]] = []
        for index, fragment in enumerate(fragments):
            overlap = cls._meaningful_tokens(fragment) & question_tokens
            if overlap:
                ranked.append((len(overlap), index, fragment))
        if not ranked:
            return ""
        chosen: List[tuple[int, str]] = []
        used = 0
        for _score, index, fragment in sorted(ranked, key=lambda item: (-item[0], item[1])):
            remaining = max_chars - used
            if remaining <= 0:
                break
            clipped = fragment[:remaining].rstrip()
            if clipped:
                chosen.append((index, clipped))
                used += len(clipped) + 2
        return "; ".join(text for _index, text in sorted(chosen))[:max_chars].rstrip()

    def _description_context(
        self,
        question: str,
        selected: Dict[str, set[int]],
        max_columns: int = 16,
        max_total_chars: int = 6000,
        prepared_index: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Render only question-relevant optional column dictionary metadata."""
        context_index = prepared_index or self._prepared_schema_context_index()
        question_tokens = self._meaningful_tokens_from_identifier_tokens(
            self._identifier_tokens(question),
        )
        if not question_tokens:
            return ""
        candidates: List[tuple[int, int, str]] = []
        empty_tokens = frozenset()
        empty_dictionary_tokens = (empty_tokens, empty_tokens, empty_tokens)
        order = 0
        for table_name, table in self.tables.items():
            table_tokens = context_index["table_meaningful_tokens"][table_name]
            effective_question_tokens = question_tokens - table_tokens
            for index, column in enumerate(table.columns):
                if index not in selected.get(table_name, set()):
                    continue
                name_tokens, description_tokens, value_tokens = context_index[
                    "column_dictionary_tokens"
                ].get((table_name, index), empty_dictionary_tokens)
                overlap = (
                    name_tokens | description_tokens | value_tokens
                ) & effective_question_tokens
                strong_single = any(
                    (token.isdigit() and token in value_tokens)
                    or (len(token) >= 4 and token in (name_tokens | value_tokens))
                    for token in overlap
                )
                if len(overlap) < 2 and not strong_single:
                    order += 1
                    continue
                fields: List[str] = []
                business_name = self._clean_description(column.semantic_name)[:120]
                if business_name:
                    fields.append("business name: " + business_name)
                meaning = self._relevant_description_excerpt(
                    column.description, effective_question_tokens, 360,
                )
                if meaning:
                    fields.append("meaning: " + meaning)
                values = self._relevant_description_excerpt(
                    column.value_description, effective_question_tokens, 900,
                )
                if values:
                    fields.append("value definitions: " + values)
                if fields:
                    candidates.append((len(overlap), order, (
                        f"COLUMN {table_name}.{column.name} | " + " | ".join(fields)
                    )))
                order += 1
        lines: List[tuple[int, str]] = []
        used = 0
        for _score, order, line in sorted(candidates, key=lambda item: (-item[0], item[1])):
            if len(lines) >= max_columns or used >= max_total_chars:
                break
            clipped = line[: max_total_chars - used].rstrip()
            if clipped:
                lines.append((order, clipped))
                used += len(clipped) + 1
        if not lines:
            return ""
        return "QUESTION-RELEVANT COLUMN DICTIONARY:\n" + "\n".join(
            line for _order, line in sorted(lines)
        )

    def compact_for_question(
        self,
        question: str,
        max_total_columns: int = 384,
    ) -> str:
        """Render a lossless small-schema context and a bounded large-schema view.

        The previous fixed first-12-columns slice silently hid valid fields even
        when the complete schema was small.  Small and medium schemas now keep
        every field.  Oversized schemas share one global column budget, ranking
        explicit question matches and declared PK/FK fields first while still
        retaining every table name and an omission count.
        """
        if max_total_columns < 1:
            raise ValueError("max_total_columns 必须大于 0")
        context_index = self._prepared_schema_context_index()
        total_columns = context_index["total_columns"]
        if total_columns <= max_total_columns:
            base = self.compact(max(
                (len(table.columns) for table in self.tables.values()),
                default=0,
            ))
            selected = {
                table_name: set(range(len(table.columns)))
                for table_name, table in self.tables.items()
            }
            descriptions = self._description_context(
                question, selected, prepared_index=context_index,
            )
            return base + ("\n\n" + descriptions if descriptions else "")

        question_folded = str(question or "").casefold()
        question_compact = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", question_folded)
        question_tokens = self._identifier_tokens(question)
        question_canonical_tokens = self._canonical_tokens_from_identifier_tokens(
            question_tokens,
        )
        question_meaningful = self._meaningful_tokens_from_identifier_tokens(
            question_tokens,
        )
        table_tokens_by_name = context_index["table_tokens"]
        table_canonical_tokens_by_name = context_index["table_canonical_tokens"]
        table_meaningful_tokens_by_name = context_index["table_meaningful_tokens"]
        table_compact_by_name = context_index["table_compact"]
        distinctive_table_tokens_by_name = context_index[
            "distinctive_table_tokens"
        ]
        column_tokens_by_position = context_index["column_tokens"]
        column_canonical_tokens_by_position = context_index[
            "column_canonical_tokens"
        ]
        column_compact_by_position = context_index["column_compact"]
        column_metadata_tokens_by_position = context_index[
            "column_metadata_tokens"
        ]
        distinctive_column_tokens_by_position = context_index[
            "distinctive_column_tokens"
        ]
        candidates: List[tuple[int, int, str, int, bool]] = []
        matched_tables: List[str] = []
        empty_tokens = frozenset()
        for table_order, (table_name, table) in enumerate(self.tables.items()):
            table_tokens = table_tokens_by_name[table_name]
            table_compact = table_compact_by_name[table_name]
            distinctive_table_tokens = distinctive_table_tokens_by_name[table_name]
            exact_table_match = bool(
                table_canonical_tokens_by_name[table_name]
                and table_canonical_tokens_by_name[table_name]
                <= question_canonical_tokens
            )
            if any(not token.isascii() for token in table_tokens):
                exact_table_match = exact_table_match or bool(
                    len(table_compact) >= 2
                    and table_compact in question_compact
                )
            table_match = bool(
                exact_table_match
                or distinctive_table_tokens & question_tokens
            )
            if table_match:
                matched_tables.append(table_name)
            effective_question_meaningful = (
                question_meaningful - table_meaningful_tokens_by_name[table_name]
            )
            for column_order, column in enumerate(table.columns):
                position = (table_name, column_order)
                column_tokens = column_tokens_by_position[position]
                column_compact = column_compact_by_position[position]
                distinctive_column_tokens = (
                    distinctive_column_tokens_by_position[position]
                )
                exact_column_match = bool(
                    column_canonical_tokens_by_position[(table_name, column_order)]
                    and column_canonical_tokens_by_position[(table_name, column_order)]
                    <= question_canonical_tokens
                )
                if any(not token.isascii() for token in column_tokens):
                    exact_column_match = exact_column_match or bool(
                        column_compact
                        and column_compact in question_compact
                    )
                distinctive_column_match = bool(
                    distinctive_column_tokens & question_tokens
                )
                score = max(0, 12 - column_order)
                if exact_column_match:
                    score += 120
                score += 35 * len(column_tokens & question_tokens)
                if table_match:
                    score += 20
                if column.pk:
                    score += 45
                if column.fk_table:
                    score += 40
                metadata_overlap = (
                    column_metadata_tokens_by_position.get(position, empty_tokens)
                    & effective_question_meaningful
                )
                score += min(120, 24 * len(metadata_overlap))
                candidates.append((
                    score, -table_order, table_name, column_order,
                    bool(
                        exact_column_match
                        or distinctive_column_match
                        or metadata_overlap
                    ),
                ))

        selected: Dict[str, set[int]] = {name: set() for name in self.tables}
        used = 0

        def reserve(table_name: str, column_order: int) -> bool:
            nonlocal used
            if (
                used >= max_total_columns
                or table_name not in selected
                or not 0 <= column_order < len(self.tables[table_name].columns)
                or column_order in selected[table_name]
            ):
                return False
            selected[table_name].add(column_order)
            used += 1
            return True

        # Relation-path columns outrank decorative context.  Only a unique
        # shortest declared path between a bounded set of confidently matched
        # tables receives this authority.
        if 1 < len(matched_tables) <= 8:
            fk_adjacency = self._prepared_fk_adjacency(context_index)
            for left_index, left_table in enumerate(matched_tables):
                for right_table in matched_tables[left_index + 1:]:
                    for table_name, column_order in self._unique_shortest_fk_path_columns(
                        left_table, right_table, adjacency=fk_adjacency,
                    ):
                        reserve(table_name, column_order)

        ranked_candidates = sorted(
            candidates,
            key=lambda item: (-item[0], -item[1], item[3]),
        )
        # Explicit/rare question matches cannot be displaced by hundreds of
        # unrelated primary keys.  Cap the reserved slice so a broad question
        # cannot consume the whole schema budget.
        question_reserved = 0
        for _score, _table_priority, table_name, column_order, relevant in (
            item for item in ranked_candidates if item[4]
        ):
            if used >= max_total_columns or question_reserved >= 64:
                break
            if relevant and reserve(table_name, column_order):
                question_reserved += 1
        for table_name in matched_tables:
            for column_order, column in enumerate(self.tables[table_name].columns):
                if column.pk:
                    reserve(table_name, column_order)

        if len(self.tables) <= max_total_columns:
            for table_name, table in self.tables.items():
                if table.columns:
                    reserve(table_name, 0)
        for _score, _table_priority, table_name, column_order, _relevant \
                in ranked_candidates:
            if used >= max_total_columns:
                break
            reserve(table_name, column_order)

        lines: List[str] = []
        compact_tables: List[str] = []
        for table_name, table in self.tables.items():
            indexes = selected[table_name]
            rendered = [
                self._column_text(column)
                for index, column in enumerate(table.columns)
                if index in indexes
            ]
            omitted = len(table.columns) - len(rendered)
            if not rendered:
                compact_tables.append(
                    f"{table_name}(+{omitted} columns)"
                )
                continue
            if omitted:
                rendered.append(f"... (+{omitted} columns omitted)")
            lines.append(
                f"TABLE {table_name} ({table.row_count} rows) | " + ", ".join(rendered)
            )
        if compact_tables:
            prefix = "TABLE INDEX (details omitted): "
            chunk: List[str] = []
            chunk_length = len(prefix)
            for entry in compact_tables:
                added = len(entry) + (2 if chunk else 0)
                if chunk and chunk_length + added > 1600:
                    lines.append(prefix + ", ".join(chunk))
                    chunk = []
                    chunk_length = len(prefix)
                chunk.append(entry)
                chunk_length += added
            if chunk:
                lines.append(prefix + ", ".join(chunk))
        base = "\n".join(lines)
        descriptions = self._description_context(
            question, selected, prepared_index=context_index,
        )
        return base + ("\n\n" + descriptions if descriptions else "")

    def l1_index(self) -> List[dict]:
        """Return a compact program-side table index."""
        return [
            {"table": t.name, "columns": [c.name for c in t.columns], "rows": t.row_count}
            for t in self.tables.values()
        ]


@dataclass
class SemanticResolution:
    """一次自然语言语义解析结果；只包含经 schema 校验的结构化命中。"""
    original_question: str
    resolved_question: str
    matches: List[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "version": "2.8",
            "engine": "native",
            "original_question": self.original_question,
            "resolved_question": self.resolved_question,
            "matches": list(self.matches),
        }


@dataclass
class CalendarFilterPlan:
    """结构化业务日历的确定性日期过滤计划。"""
    mode: str
    table: str
    column: str
    calendar_term: str
    date_range: Dict[str, Any]
    predicate: str
    rules: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.2"
    engine: str = "native_calendar"
    dialect: str = "sqlite"
    status: str = "compiled"
    sql: Optional[str] = None

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "engine": self.engine,
            "dialect": self.dialect,
            "mode": self.mode,
            "table": self.table,
            "column": self.column,
            "calendar_term": self.calendar_term,
            "date_range": dict(self.date_range),
            "rules": dict(self.rules),
            "predicate": self.predicate,
            "status": self.status,
            "sql": self.sql,
        }


@dataclass
class MultiMetricAggregatePlan:
    """同表多个受控普通指标的一次性确定性聚合计划。"""
    table: str
    measures: List[dict]
    global_filters: List[dict] = field(default_factory=list)
    version: str = "1.0"
    engine: str = "native_multi_metric"
    dialect: str = "sqlite"
    status: str = "compiled"
    sql: Optional[str] = None

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "engine": self.engine,
            "dialect": self.dialect,
            "table": self.table,
            "measures": [dict(item) for item in self.measures],
            "global_filters": [dict(item) for item in self.global_filters],
            "status": self.status,
            "sql": self.sql,
        }


@dataclass
class DimensionAggregatePlan:
    """同表业务维度的确定性分组/下钻聚合计划。"""
    mode: str
    table: str
    dimensions: List[dict]
    measure: Dict[str, Any]
    measures: List[dict] = field(default_factory=list)
    hierarchy: Optional[dict] = None
    filters: List[dict] = field(default_factory=list)
    dimension_filters: List[dict] = field(default_factory=list)
    global_filters: List[dict] = field(default_factory=list)
    version: str = "1.2"
    engine: str = "native_dimension"
    dialect: str = "sqlite"
    status: str = "compiled"
    sql: Optional[str] = None

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "engine": self.engine,
            "dialect": self.dialect,
            "mode": self.mode,
            "table": self.table,
            "dimensions": [dict(item) for item in self.dimensions],
            "measure": dict(self.measure),
            "measures": [dict(item) for item in self.measures],
            "hierarchy": dict(self.hierarchy) if self.hierarchy else None,
            "filters": [dict(item) for item in self.filters],
            "dimension_filters": [dict(item) for item in self.dimension_filters],
            "global_filters": [dict(item) for item in self.global_filters],
            "status": self.status,
            "sql": self.sql,
        }


@dataclass
class TrendAggregatePlan:
    """单表时间字段的确定性趋势聚合计划。"""
    table: str
    column: str
    time_term: str
    grain: str
    grain_source: str
    bucket: Dict[str, Any]
    measure: Dict[str, Any]
    measures: List[dict] = field(default_factory=list)
    filters: List[dict] = field(default_factory=list)
    global_filters: List[dict] = field(default_factory=list)
    date_range: Optional[Dict[str, Any]] = None
    rules: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.3"
    engine: str = "native_trend"
    dialect: str = "sqlite"
    status: str = "compiled"
    sql: Optional[str] = None

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "engine": self.engine,
            "dialect": self.dialect,
            "table": self.table,
            "column": self.column,
            "time_term": self.time_term,
            "grain": self.grain,
            "grain_source": self.grain_source,
            "bucket": dict(self.bucket),
            "measure": dict(self.measure),
            "measures": [dict(item) for item in self.measures],
            "filters": [dict(item) for item in self.filters],
            "global_filters": [dict(item) for item in self.global_filters],
            "date_range": dict(self.date_range) if self.date_range else None,
            "rules": dict(self.rules),
            "status": self.status,
            "sql": self.sql,
        }


class SemanticCatalog:
    """按 schema 校验业务术语，并为规划器与提示词生成稳定的语义上下文。"""

    KINDS = frozenset({
        "table_alias", "column_alias", "enum_value", "metric", "ratio_metric",
        "dimension", "time_field", "business_calendar",
    })
    AGGREGATIONS = frozenset({"count", "count_distinct", "sum", "avg", "min", "max"})
    FILTER_OPERATORS = frozenset({
        "eq", "neq", "gt", "gte", "lt", "lte", "in", "not_in",
        "is_null", "is_not_null",
    })
    FILTER_OPERATOR_LABELS = {
        "eq": "等于", "neq": "不等于", "gt": "大于", "gte": "大于等于",
        "lt": "小于", "lte": "小于等于", "in": "属于", "not_in": "不属于",
        "is_null": "为空", "is_not_null": "不为空",
    }
    TIME_GRAINS = frozenset({"day", "week", "month", "quarter", "year"})
    TIME_GRAIN_LABELS = {
        "day": "日", "week": "周", "month": "月", "quarter": "季度", "year": "年",
    }
    MAX_ENTRIES = 200
    MAX_FILTERS = 4
    MAX_FILTER_VALUES = 20
    BUSINESS_CALENDAR_SIGNAL_RE = re.compile(
        r"(财年|财季|会计年度|会计季度|工作日|交易日|营业日|"
        r"fiscal\s+(?:year|quarter)|business\s+days?|trading\s+days?)",
        re.IGNORECASE,
    )
    TIME_ANALYSIS_SIGNAL_RE = re.compile(
        r"(最近|近期|过去|趋势|同比|环比|按日|按周|按月|按季度|按年|"
        r"日期|时间|\bdate\b|\btime\b|\btrend\b)",
        re.IGNORECASE,
    )
    DIMENSION_DRILL_SIGNAL_RE = re.compile(r"(下钻|钻取|细分|展开)", re.IGNORECASE)
    DIMENSION_NEXT_LEVEL_RE = re.compile(
        r"(下钻|钻取|细分|展开)\s*(?:一|1)\s*(?:级|层)|下一(?:级|层)",
        re.IGNORECASE,
    )

    def __init__(
        self,
        schema: SchemaSnapshot,
        entries: Optional[List[dict]] = None,
        strict: bool = False,
    ):
        self.schema = schema
        source = entries or []
        if len(source) > self.MAX_ENTRIES:
            raise ValueError(f"语义定义不能超过 {self.MAX_ENTRIES} 项")
        self.entries: List[dict] = []
        self.invalid_entries: List[dict] = []
        self._by_term: Dict[str, dict] = {}
        self._hierarchy_levels: Dict[str, dict] = {}
        self._time_field_defaults: Dict[tuple[str, str], str] = {}
        for raw in source:
            try:
                entry = self.validate_entry(raw)
                key = entry["term"].casefold()
                if key in self._by_term:
                    raise ValueError(f"语义术语重复: {entry['term']}")
                self._register_hierarchy(entry)
                self._register_time_default(entry)
                self._by_term[key] = entry
                self.entries.append(entry)
            except ValueError as exc:
                if strict:
                    raise
                self.invalid_entries.append({"entry": raw, "error": str(exc)})

    def _register_hierarchy(self, entry: dict) -> None:
        if entry.get("kind") != "dimension" or not entry.get("hierarchy"):
            return
        hierarchy = entry["hierarchy"]
        key = hierarchy["name"].casefold()
        group = self._hierarchy_levels.setdefault(
            key,
            {"name": hierarchy["name"], "table": entry["table"], "levels": {}},
        )
        if group["table"] != entry["table"]:
            raise ValueError(
                f"维度层级“{hierarchy['name']}”只能绑定同一张表，"
                f"不能同时使用 {group['table']} 和 {entry['table']}"
            )
        level = hierarchy["level"]
        if level in group["levels"]:
            raise ValueError(
                f"维度层级“{hierarchy['name']}”的第 {level} 级已经由"
                f"“{group['levels'][level]}”占用"
            )
        group["levels"][level] = entry["term"]

    def _hierarchy_path(self, hierarchy_name: str) -> List[dict]:
        key = str(hierarchy_name or "").casefold()
        return [
            {
                "level": entry["hierarchy"]["level"],
                "term": entry["term"],
                "table": entry["table"],
                "column": entry["column"],
                "filters": json.loads(json.dumps(entry.get("filters") or [], ensure_ascii=False)),
            }
            for entry in sorted(
                (
                    item for item in self.entries
                    if item.get("kind") == "dimension"
                    and item.get("hierarchy")
                    and item["hierarchy"]["name"].casefold() == key
                ),
                key=lambda item: item["hierarchy"]["level"],
            )
        ]

    def dimension_drill_request(self, question: str) -> Optional[dict]:
        """解析显式同表下钻请求；目标不明确时返回可用的更深层级。"""
        signal = self.DIMENSION_DRILL_SIGNAL_RE.search(question)
        if signal is None:
            return None
        dimensions = [
            item for item in self.resolve(question).matches
            if item.get("kind") == "dimension" and item.get("hierarchy")
        ]
        positioned = []
        folded = question.casefold()
        for item in dimensions:
            position = folded.find(str(item.get("term") or "").casefold())
            if position >= 0:
                positioned.append((position, item))
        positioned.sort(key=lambda pair: pair[0])
        before = [item for position, item in positioned if position < signal.start()]
        after = [item for position, item in positioned if position >= signal.end()]
        source = before[-1] if before else None
        target = after[0] if after else None

        labelled = re.search(r"维度层级\s*[:：]\s*([^；;，,。]+)", question, re.IGNORECASE)
        if labelled:
            label_text = labelled.group(1).casefold()
            labelled_targets = [
                item for item in dimensions
                if str(item.get("term") or "").casefold() in label_text
            ]
            if len(labelled_targets) == 1:
                target = labelled_targets[0]

        if source is None:
            return {
                "status": "needs_source",
                "source": None,
                "target": target,
                "dimensions": [],
                "candidates": [],
            }
        path = list(source.get("hierarchy_path") or [])
        source_level = int(source["hierarchy"]["level"])
        candidates = [item for item in path if int(item["level"]) > source_level]
        if target is None and self.DIMENSION_NEXT_LEVEL_RE.search(question):
            immediate = [item for item in candidates if int(item["level"]) == source_level + 1]
            if len(immediate) == 1:
                target_term = immediate[0]["term"]
                target = next(
                    (
                        self._resolved_match(entry) for entry in self.entries
                        if entry.get("kind") == "dimension" and entry["term"] == target_term
                    ),
                    None,
                )
        if target is None:
            return {
                "status": "needs_level",
                "source": source,
                "target": None,
                "dimensions": [],
                "candidates": candidates,
            }
        same_path = (
            target.get("table") == source.get("table")
            and target.get("hierarchy", {}).get("name", "").casefold()
            == source.get("hierarchy", {}).get("name", "").casefold()
        )
        target_level = int(target.get("hierarchy", {}).get("level") or 0)
        if not same_path or target_level <= source_level:
            return {
                "status": "invalid_target",
                "source": source,
                "target": target,
                "dimensions": [],
                "candidates": candidates,
            }
        selected = [
            item for item in path
            if source_level <= int(item["level"]) <= target_level
        ]
        return {
            "status": "resolved",
            "source": source,
            "target": target,
            "dimensions": selected,
            "candidates": candidates,
        }

    def _register_time_default(self, entry: dict) -> None:
        if entry.get("kind") != "time_field" or not entry.get("default_grain"):
            return
        key = (entry["table"], entry["column"])
        previous = self._time_field_defaults.get(key)
        if previous and previous != entry["default_grain"]:
            raise ValueError(
                f"时间字段 {entry['table']}.{entry['column']} 不能配置冲突的默认粒度"
            )
        self._time_field_defaults[key] = entry["default_grain"]

    @staticmethod
    def _clean_text(value: Any, field_name: str, max_length: int) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError(f"{field_name}不能为空")
        if len(text) > max_length:
            raise ValueError(f"{field_name}不能超过 {max_length} 个字符")
        if any(ord(char) < 32 for char in text):
            raise ValueError(f"{field_name}包含控制字符")
        return text

    def _table(self, name: Any) -> DBTable:
        table_name = self._clean_text(name, "目标表", 128)
        table = self.schema.tables.get(table_name)
        if table is None:
            raise ValueError(f"目标表不存在: {table_name}")
        return table

    @staticmethod
    def _column(table: DBTable, name: Any) -> DBColumn:
        column_name = SemanticCatalog._clean_text(name, "目标字段", 128)
        column = next((item for item in table.columns if item.name == column_name), None)
        if column is None:
            raise ValueError(f"字段 {column_name} 不存在于表 {table.name}")
        return column

    @staticmethod
    def _is_numeric_column(column: DBColumn) -> bool:
        return bool(re.search(
            r"(INT|REAL|NUMERIC|DECIMAL|FLOAT|DOUBLE|NUMBER)",
            column.type or "",
            re.IGNORECASE,
        ))

    @staticmethod
    def _is_boolean_column(column: DBColumn) -> bool:
        return bool(re.search(r"BOOL", column.type or "", re.IGNORECASE))

    def _filter_scalar(self, column: DBColumn, value: Any, label: str) -> Any:
        if value is None or isinstance(value, (dict, list)):
            raise ValueError(f"{label}必须是字符串、数字或布尔值")
        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise ValueError(f"{label}不能为空")
            if len(value) > 240:
                raise ValueError(f"{label}不能超过 240 个字符")
            if any(ord(char) < 32 for char in value):
                raise ValueError(f"{label}包含控制字符")
        elif isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"{label}必须是有限数值")
        elif not isinstance(value, (int, float, bool)):
            raise ValueError(f"{label}必须是字符串、数字或布尔值")
        if self._is_numeric_column(column) and (
            isinstance(value, bool) or not isinstance(value, (int, float))
        ):
            raise ValueError(f"字段 {column.name} 的过滤值必须是数字")
        if self._is_boolean_column(column) and not isinstance(value, bool):
            raise ValueError(f"字段 {column.name} 的过滤值必须是布尔值")
        return value

    def _metric_filters(self, table: DBTable, raw: Any, label: str) -> List[dict]:
        if raw in (None, []):
            return []
        if not isinstance(raw, list):
            raise ValueError(f"{label}过滤条件必须是结构化列表")
        if len(raw) > self.MAX_FILTERS:
            raise ValueError(f"{label}过滤条件不能超过 {self.MAX_FILTERS} 条")
        filters = []
        for index, item in enumerate(raw, start=1):
            item_label = f"{label}第 {index} 条过滤条件"
            if not isinstance(item, dict):
                raise ValueError(f"{item_label}必须是对象")
            extra = set(item) - {"column", "operator", "value"}
            if extra:
                raise ValueError(f"{item_label}只支持 column、operator 和 value")
            column = self._column(table, item.get("column"))
            operator = str(item.get("operator") or "").strip().lower()
            if operator not in self.FILTER_OPERATORS:
                raise ValueError(
                    f"{item_label}操作符只支持 eq/neq/gt/gte/lt/lte/in/not_in/is_null/is_not_null"
                )
            if operator in {"gt", "gte", "lt", "lte"} and not (
                self._is_numeric_column(column) or is_time_column(column)
            ):
                raise ValueError(f"{item_label}的范围比较只支持数值或时间字段")
            if operator in {"is_null", "is_not_null"}:
                value = None
            elif operator in {"in", "not_in"}:
                raw_values = item.get("value")
                if not isinstance(raw_values, list) or not raw_values:
                    raise ValueError(f"{item_label}必须提供非空值列表")
                if len(raw_values) > self.MAX_FILTER_VALUES:
                    raise ValueError(
                        f"{item_label}值列表不能超过 {self.MAX_FILTER_VALUES} 项"
                    )
                value = [
                    self._filter_scalar(column, item_value, f"{item_label}值")
                    for item_value in raw_values
                ]
            else:
                value = self._filter_scalar(column, item.get("value"), f"{item_label}值")
            filters.append({"column": column.name, "operator": operator, "value": value})
        return filters

    def _metric_component(self, table: DBTable, raw: Any, label: str) -> dict:
        if not isinstance(raw, dict):
            raise ValueError(f"比率指标{label}必须是受控聚合定义")
        extra = set(raw) - {"aggregation", "column", "filters"}
        if extra:
            raise ValueError(f"比率指标{label}只支持 aggregation、column 和 filters")
        aggregation = str(raw.get("aggregation") or "").strip().lower()
        if aggregation not in self.AGGREGATIONS:
            raise ValueError(
                f"比率指标{label}聚合只支持 count/count_distinct/sum/avg/min/max"
            )
        column_name = ""
        if aggregation != "count":
            column = self._column(table, raw.get("column"))
            if aggregation in {"sum", "avg", "min", "max"} and not self._is_numeric_column(column):
                raise ValueError(f"比率指标{label}的 {aggregation} 字段必须是数值类型")
            column_name = column.name
        return {
            "aggregation": aggregation,
            "column": column_name,
            "filters": self._metric_filters(table, raw.get("filters"), f"比率指标{label}"),
        }

    def _ratio_formula(self, table: DBTable, raw: Any) -> dict:
        if not isinstance(raw, dict):
            raise ValueError("比率指标必须提供受控公式")
        extra = set(raw) - {"operator", "numerator", "denominator", "scale", "zero_division"}
        if extra:
            raise ValueError("比率指标公式包含不支持的字段")
        operator = str(raw.get("operator") or "divide").strip().lower()
        if operator != "divide":
            raise ValueError("比率指标只支持分子除以分母")
        zero_division = str(raw.get("zero_division") or "null").strip().lower()
        if zero_division != "null":
            raise ValueError("比率指标分母为零时只允许返回 NULL")
        scale = raw.get("scale", 1)
        if isinstance(scale, bool) or not isinstance(scale, (int, float)) or float(scale) not in {1.0, 100.0}:
            raise ValueError("比率指标结果倍率只支持 1 或 100")
        return {
            "operator": "divide",
            "numerator": self._metric_component(table, raw.get("numerator"), "分子"),
            "denominator": self._metric_component(table, raw.get("denominator"), "分母"),
            "scale": int(scale),
            "zero_division": "null",
        }

    def _dimension_hierarchy(self, raw: Any) -> Optional[dict]:
        if raw in (None, {}, ""):
            return None
        if not isinstance(raw, dict) or set(raw) - {"name", "level"}:
            raise ValueError("维度层级只支持 name 和 level")
        name = self._clean_text(raw.get("name"), "维度层级名称", 80)
        level = raw.get("level")
        if isinstance(level, bool) or not isinstance(level, int) or not 1 <= level <= 8:
            raise ValueError("维度层级顺序必须是 1 到 8 的整数")
        return {"name": name, "level": level}

    def _default_time_grain(self, raw: Any) -> str:
        grain = str(raw or "").strip().lower()
        if grain and grain not in self.TIME_GRAINS:
            raise ValueError("时间默认粒度只支持 day/week/month/quarter/year")
        return grain

    def _business_calendar(self, raw: Any, bound_column: DBColumn) -> dict:
        if not isinstance(raw, dict):
            raise ValueError("业务日历必须提供结构化口径")
        allowed = {
            "fiscal_year_start_month", "fiscal_year_start_day", "timezone",
            "fiscal_year_label", "fiscal_year_label_source",
            "storage_basis", "storage_basis_source", "timezone_conversion",
            "business_utc_offset_minutes", "tzdata_version", "iana_version",
            "week_start", "weekend_days", "holiday_table", "holiday_date_column",
            "holiday_name_column", "working_override_column",
        }
        if set(raw) - allowed:
            raise ValueError("业务日历包含不支持的字段")

        def controlled_int(name: str, default: int, minimum: int, maximum: int) -> int:
            value = raw.get(name, default)
            if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
                raise ValueError(f"业务日历 {name} 必须是 {minimum} 到 {maximum} 的整数")
            return value

        fiscal_month = controlled_int("fiscal_year_start_month", 1, 1, 12)
        fiscal_day = controlled_int(
            "fiscal_year_start_day", 1, 1, pycalendar.monthrange(2001, fiscal_month)[1],
        )
        week_start = controlled_int("week_start", 1, 1, 7)
        weekend_raw = raw.get("weekend_days", [6, 7])
        if not isinstance(weekend_raw, list):
            raise ValueError("业务日历 weekend_days 必须是 ISO 星期列表")
        if any(
            isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 7
            for value in weekend_raw
        ):
            raise ValueError("业务日历 weekend_days 只接受 1 到 7 的整数")
        weekend_days = sorted(set(weekend_raw))
        if len(weekend_days) != len(weekend_raw):
            raise ValueError("业务日历 weekend_days 不能重复")
        if len(weekend_days) > 6:
            raise ValueError("业务日历必须至少保留一个工作日")

        timezone = self._clean_text(raw.get("timezone") or "UTC", "业务日历时区", 64)
        if timezone != "UTC" and not re.fullmatch(
            r"[A-Za-z][A-Za-z0-9_+.-]*/[A-Za-z0-9_+.-]+(?:/[A-Za-z0-9_+.-]+)*",
            timezone,
        ):
            raise ValueError("业务日历时区必须是 UTC 或 IANA 风格标识，例如 Asia/Shanghai")
        fiscal_year_label_present = "fiscal_year_label" in raw
        fiscal_year_label = str(raw.get("fiscal_year_label") or "start_year").strip().lower()
        if fiscal_year_label not in {"start_year", "end_year"}:
            raise ValueError("业务日历 fiscal_year_label 只支持 start_year 或 end_year")
        fiscal_year_label_source = str(
            raw.get("fiscal_year_label_source")
            or ("explicit" if fiscal_year_label_present else "legacy_default")
        ).strip().lower()
        if fiscal_year_label_source not in {"explicit", "legacy_default"}:
            raise ValueError("业务日历 fiscal_year_label_source 无效")
        if fiscal_year_label_source == "explicit" and not fiscal_year_label_present:
            raise ValueError("显式财年年份标注必须提供 fiscal_year_label")

        declared_type = re.sub(r"\s+", " ", str(bound_column.type or "").strip()).upper()
        is_declared_date = declared_type == "DATE"
        is_declared_timestamp = declared_type in {"DATETIME", "TIMESTAMP"}
        storage_basis_present = "storage_basis" in raw
        storage_basis = str(
            raw.get("storage_basis")
            or ("declared_date" if is_declared_date else "unspecified")
        ).strip().lower()
        if storage_basis not in {
            "unspecified", "declared_date", "local_datetime", "utc_datetime",
        }:
            raise ValueError(
                "业务日历 storage_basis 只支持 unspecified/declared_date/"
                "local_datetime/utc_datetime"
            )
        storage_basis_source = str(
            raw.get("storage_basis_source")
            or (
                "explicit" if storage_basis_present
                else ("schema_inferred" if is_declared_date else "legacy_default")
            )
        ).strip().lower()
        if storage_basis_source not in {"explicit", "schema_inferred", "legacy_default"}:
            raise ValueError("业务日历 storage_basis_source 无效")
        if storage_basis_source == "schema_inferred" and not (
            storage_basis == "declared_date" and is_declared_date
        ):
            raise ValueError("只有声明型 DATE 字段可以由 schema 推断存储基准")
        if storage_basis_source == "legacy_default" and storage_basis != "unspecified":
            raise ValueError("旧配置默认存储基准必须保持 unspecified")
        if storage_basis == "declared_date" and not is_declared_date:
            raise ValueError("declared_date 存储基准只适用于声明型 DATE 字段")
        if storage_basis in {"local_datetime", "utc_datetime"} and not is_declared_timestamp:
            raise ValueError(
                "时间戳存储基准只适用于声明型 DATETIME 或 TIMESTAMP 字段"
            )

        timezone_conversion = str(
            raw.get("timezone_conversion")
            or ("fixed_offset" if storage_basis == "utc_datetime" else "none")
        ).strip().lower()
        if timezone_conversion not in {"none", "fixed_offset", "iana_tzdata"}:
            raise ValueError(
                "业务日历 timezone_conversion 只支持 none/fixed_offset/iana_tzdata"
            )

        business_utc_offset_minutes: Optional[int] = None
        tzdata_version: Optional[str] = None
        iana_version: Optional[str] = None
        raw_offset = raw.get("business_utc_offset_minutes")
        if storage_basis == "utc_datetime":
            if timezone_conversion == "fixed_offset":
                if isinstance(raw_offset, bool) or not isinstance(raw_offset, int):
                    raise ValueError("UTC 时间戳固定偏移换日必须提供整数 business_utc_offset_minutes")
                if not -840 <= raw_offset <= 840:
                    raise ValueError("业务 UTC 固定偏移必须在 -840 到 840 分钟之间")
                if timezone == "UTC" and raw_offset != 0:
                    raise ValueError("业务日历时区为 UTC 时固定偏移必须为 0 分钟")
                business_utc_offset_minutes = raw_offset
            elif timezone_conversion == "iana_tzdata":
                if raw_offset is not None:
                    raise ValueError("IANA 动态换日不能同时设置 business_utc_offset_minutes")
                if timezone == "UTC" or "/" not in timezone:
                    raise ValueError("IANA 动态换日必须使用真实区域时区，例如 America/New_York")
                _, release = TimezoneRuntime.resolve_release(
                    tzdata_version=str(raw.get("tzdata_version") or ""),
                    iana_version=str(raw.get("iana_version") or ""),
                )
                TimezoneRuntime.require_zone(
                    timezone,
                    tzdata_version=release["tzdata_version"],
                    iana_version=release["iana_version"],
                )
                tzdata_version = release["tzdata_version"]
                iana_version = release["iana_version"]
            else:
                raise ValueError("utc_datetime 必须选择 fixed_offset 或 iana_tzdata 换日")
        elif raw_offset is not None:
            raise ValueError("只有 utc_datetime 存储基准可以设置业务 UTC 固定偏移")
        elif timezone_conversion != "none":
            raise ValueError("只有 utc_datetime 存储基准可以执行时区换日")
        elif raw.get("tzdata_version") is not None or raw.get("iana_version") is not None:
            raise ValueError("只有 IANA 动态换日可以设置时区数据版本")

        holiday_table_name = str(raw.get("holiday_table") or "").strip()
        holiday_date_column = ""
        holiday_name_column = ""
        working_override_column = ""
        if holiday_table_name:
            holiday_table = self._table(holiday_table_name)
            date_column = self._column(holiday_table, raw.get("holiday_date_column"))
            if not is_time_column(date_column):
                raise ValueError("节假日日期字段必须具有日期/时间类型或可识别的时间字段名")
            holiday_date_column = date_column.name
            if raw.get("holiday_name_column"):
                holiday_name_column = self._column(
                    holiday_table, raw.get("holiday_name_column"),
                ).name
            if raw.get("working_override_column"):
                override = self._column(holiday_table, raw.get("working_override_column"))
                if not (
                    self._is_boolean_column(override) or self._is_numeric_column(override)
                ):
                    raise ValueError("工作日覆盖字段必须是布尔或数值类型")
                working_override_column = override.name
            holiday_table_name = holiday_table.name
        elif any(
            str(raw.get(name) or "").strip()
            for name in ("holiday_date_column", "holiday_name_column", "working_override_column")
        ):
            raise ValueError("绑定节假日字段前必须先选择节假日表")

        return {
            "fiscal_year_start_month": fiscal_month,
            "fiscal_year_start_day": fiscal_day,
            "fiscal_year_label": fiscal_year_label,
            "fiscal_year_label_source": fiscal_year_label_source,
            "timezone": timezone,
            "storage_basis": storage_basis,
            "storage_basis_source": storage_basis_source,
            "timezone_conversion": timezone_conversion,
            "business_utc_offset_minutes": business_utc_offset_minutes,
            "tzdata_version": tzdata_version,
            "iana_version": iana_version,
            "week_start": week_start,
            "weekend_days": weekend_days,
            "holiday_table": holiday_table_name,
            "holiday_date_column": holiday_date_column,
            "holiday_name_column": holiday_name_column,
            "working_override_column": working_override_column,
        }

    @classmethod
    def _filters_text(cls, table_name: str, filters: List[dict]) -> str:
        parts = []
        for item in filters:
            operator = cls.FILTER_OPERATOR_LABELS.get(item["operator"], item["operator"])
            if item["operator"] in {"is_null", "is_not_null"}:
                parts.append(f"{table_name}.{item['column']} {operator}")
            else:
                parts.append(
                    f"{table_name}.{item['column']} {operator} "
                    f"{json.dumps(item['value'], ensure_ascii=False)}"
                )
        return " 且 ".join(parts)

    @classmethod
    def _component_text(cls, table_name: str, component: dict) -> str:
        aggregation = component["aggregation"]
        target = (
            f"{table_name}.{component['column']}"
            if component.get("column") else f"表 {table_name}"
        )
        base = f"{aggregation}({target})"
        filters = component.get("filters") or []
        return f"{base} [过滤：{cls._filters_text(table_name, filters)}]" if filters else base

    def validate_entry(self, raw: dict) -> dict:
        if not isinstance(raw, dict):
            raise ValueError("语义定义必须是对象")
        kind = str(raw.get("kind") or "").strip().lower()
        if kind not in self.KINDS:
            raise ValueError(f"语义类型不支持: {kind or '空'}")
        term = self._clean_text(raw.get("term"), "业务术语", 80)
        description = str(raw.get("description") or "").strip()[:240]
        if term.casefold() in {name.casefold() for name in self.schema.tables}:
            raise ValueError("业务术语不能与真实表名相同")

        table = self._table(raw.get("table"))
        column_name = ""
        value: Any = None
        aggregation = ""
        formula: Optional[dict] = None
        filters: List[dict] = []
        calendar: Optional[dict] = None
        hierarchy: Optional[dict] = None
        default_grain = ""
        if kind in {"column_alias", "enum_value", "dimension", "time_field", "business_calendar"}:
            column = self._column(table, raw.get("column"))
            column_name = column.name
            if term.casefold() == column.name.casefold():
                raise ValueError("业务术语不能与真实字段名相同")
            if kind in {"time_field", "business_calendar"} and not is_time_column(column):
                raise ValueError("时间字段必须具有日期/时间类型或可识别的时间字段名")
        if kind == "enum_value":
            value = raw.get("value")
            if value is None or isinstance(value, (dict, list)):
                raise ValueError("枚举映射值必须是字符串、数字或布尔值")
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    raise ValueError("枚举映射值不能为空")
                if len(value) > 240:
                    raise ValueError("枚举映射值不能超过 240 个字符")
        if kind == "metric":
            aggregation = str(raw.get("aggregation") or "").strip().lower()
            if aggregation not in self.AGGREGATIONS:
                raise ValueError("指标聚合只支持 count/count_distinct/sum/avg/min/max")
            if aggregation != "count":
                column = self._column(table, raw.get("column"))
                column_name = column.name
            filters = self._metric_filters(table, raw.get("filters"), "指标")
        if kind == "ratio_metric":
            formula = self._ratio_formula(table, raw.get("formula"))
        if kind == "dimension":
            hierarchy = self._dimension_hierarchy(raw.get("hierarchy"))
            filters = self._metric_filters(table, raw.get("filters"), "维度")
        if kind == "time_field":
            default_grain = self._default_time_grain(raw.get("default_grain"))
        if kind == "business_calendar":
            calendar = self._business_calendar(raw.get("calendar"), column)
        return {
            "id": str(raw.get("id") or "").strip()[:64],
            "kind": kind,
            "term": term,
            "table": table.name,
            "column": column_name,
            "value": value,
            "aggregation": aggregation,
            "filters": filters,
            "formula": formula,
            "calendar": calendar,
            "hierarchy": hierarchy,
            "default_grain": default_grain,
            "description": description,
        }

    @staticmethod
    def _term_spans(question: str, term: str) -> List[tuple[int, int]]:
        if re.fullmatch(r"[a-z0-9_ -]+", term, re.I):
            pattern = rf"(?<![a-z0-9_]){re.escape(term)}(?![a-z0-9_])"
        else:
            pattern = re.escape(term)
        return [match.span() for match in re.finditer(pattern, question, re.I)]

    @staticmethod
    def _entry_match(entry: dict) -> dict:
        match = {
            "kind": entry["kind"],
            "term": entry["term"],
            "table": entry["table"],
        }
        if entry["column"]:
            match["column"] = entry["column"]
        if entry["kind"] == "enum_value":
            match["value"] = entry["value"]
        if entry["kind"] in {"metric", "dimension"}:
            match["filters"] = json.loads(json.dumps(entry["filters"], ensure_ascii=False))
        if entry["kind"] == "metric":
            match["aggregation"] = entry["aggregation"]
        if entry["kind"] == "ratio_metric":
            match["formula"] = json.loads(json.dumps(entry["formula"], ensure_ascii=False))
        if entry["kind"] == "business_calendar":
            match["calendar"] = json.loads(json.dumps(entry["calendar"], ensure_ascii=False))
        if entry["kind"] == "dimension" and entry.get("hierarchy"):
            match["hierarchy"] = dict(entry["hierarchy"])
        if entry["kind"] == "time_field" and entry.get("default_grain"):
            match["default_grain"] = entry["default_grain"]
        return match

    def _resolved_match(self, entry: dict) -> dict:
        match = self._entry_match(entry)
        if entry.get("kind") == "dimension" and entry.get("hierarchy"):
            match["hierarchy_path"] = self._hierarchy_path(entry["hierarchy"]["name"])
        return match

    @staticmethod
    def _calendar_text(table_name: str, column_name: str, calendar: dict) -> str:
        storage_basis = calendar.get("storage_basis") or "unspecified"
        if storage_basis == "declared_date":
            storage_text = "声明型 DATE（不执行时区换日）"
        elif storage_basis == "local_datetime":
            storage_text = "业务本地 DATETIME/TIMESTAMP（无时区后缀，不执行换算）"
        elif storage_basis == "utc_datetime":
            if calendar.get("timezone_conversion") == "iana_tzdata":
                storage_text = (
                    "UTC DATETIME/TIMESTAMP，按 IANA 区域规则动态换日"
                    f"（tzdata {calendar.get('tzdata_version')} / IANA "
                    f"{calendar.get('iana_version')}）"
                )
            else:
                offset = int(calendar.get("business_utc_offset_minutes") or 0)
                storage_text = f"UTC DATETIME/TIMESTAMP，按固定偏移 {offset:+d} 分钟换日"
        else:
            storage_text = "未声明（不进入确定性时间戳执行）"
        text = (
            f"绑定 {table_name}.{column_name}，财年从 "
            f"{calendar['fiscal_year_start_month']:02d}-{calendar['fiscal_year_start_day']:02d} 开始，"
            f"财年按{'起始年' if calendar['fiscal_year_label'] == 'start_year' else '结束年'}标注"
            f"{'（旧配置默认，待确认）' if calendar['fiscal_year_label_source'] == 'legacy_default' else ''}，"
            f"周起始 ISO={calendar['week_start']}，周末 ISO={calendar['weekend_days']}，"
            f"时区 {calendar['timezone']}，存储基准 {storage_text}"
        )
        if calendar.get("holiday_table"):
            text += (
                f"，节假日日期 {calendar['holiday_table']}.{calendar['holiday_date_column']}"
            )
            if calendar.get("working_override_column"):
                text += f"，工作日覆盖 {calendar['holiday_table']}.{calendar['working_override_column']}"
            if calendar.get("holiday_name_column"):
                text += f"，名称 {calendar['holiday_table']}.{calendar['holiday_name_column']}"
        else:
            text += "，未绑定节假日例外表"
        return text

    def _mentioned_time_columns(self, folded_question: str) -> set[tuple[str, str]]:
        """限定名优先；只有没有 table.column 时才按裸字段名匹配。"""
        qualified = {
            (table_name, column.name)
            for table_name, table in self.schema.tables.items()
            for column in table.columns
            if is_time_column(column)
            and f"{table_name}.{column.name}".casefold() in folded_question
        }
        if qualified:
            return qualified
        return {
            (table_name, column.name)
            for table_name, table in self.schema.tables.items()
            for column in table.columns
            if is_time_column(column) and column.name.casefold() in folded_question
        }

    def resolve(self, question: str) -> SemanticResolution:
        matches = []
        occupied: List[tuple[int, int]] = []
        for entry in sorted(self.entries, key=lambda item: len(item["term"]), reverse=True):
            spans = self._term_spans(question, entry["term"])
            available = [
                span for span in spans
                if not any(span[0] < used[1] and used[0] < span[1] for used in occupied)
            ]
            if not available:
                continue
            occupied.extend(available)
            matches.append(self._resolved_match(entry))

        if self.BUSINESS_CALENDAR_SIGNAL_RE.search(question) \
                and not any(item["kind"] == "business_calendar" for item in matches):
            folded_question = question.casefold()
            context_tables = {
                str(item.get("table") or "") for item in matches
                if item.get("kind") != "business_calendar"
            }
            mentioned_time_columns = self._mentioned_time_columns(folded_question)
            candidates = []
            for entry in self.entries:
                if entry["kind"] != "business_calendar":
                    continue
                qualified = f"{entry['table']}.{entry['column']}"
                if mentioned_time_columns:
                    matched = (entry["table"], entry["column"]) in mentioned_time_columns
                else:
                    matched = (
                        entry["table"] in context_tables
                        or entry["table"].casefold() in folded_question
                        or qualified.casefold() in folded_question
                    )
                if matched:
                    candidates.append(entry)
            if len(candidates) == 1:
                matches.append(self._resolved_match(candidates[0]))

        if self.TIME_ANALYSIS_SIGNAL_RE.search(question) \
                and not any(item["kind"] == "time_field" for item in matches):
            folded_question = question.casefold()
            mentioned_time_columns = self._mentioned_time_columns(folded_question)
            candidates = [
                entry for entry in self.entries
                if entry["kind"] == "time_field"
                and (entry["table"], entry["column"]) in mentioned_time_columns
            ]
            if len(candidates) == 1:
                matches.append(self._resolved_match(candidates[0]))

        resolved = question
        if matches:
            hints = []
            for item in matches:
                if item["kind"] == "table_alias":
                    hints.append(f"术语“{item['term']}”对应表 {item['table']}")
                elif item["kind"] == "column_alias":
                    hints.append(f"术语“{item['term']}”对应字段 {item['table']}.{item['column']}")
                elif item["kind"] == "enum_value":
                    hints.append(
                        f"术语“{item['term']}”对应 {item['table']}.{item['column']} 的值 {json.dumps(item['value'], ensure_ascii=False)}"
                    )
                elif item["kind"] == "dimension":
                    hint = f"维度“{item['term']}”对应分组字段 {item['table']}.{item['column']}"
                    if item.get("hierarchy"):
                        path = " > ".join(
                            f"L{level['level']} {level['term']}({level['table']}.{level['column']})"
                            for level in item.get("hierarchy_path") or []
                        )
                        hint += (
                            f"，属于层级“{item['hierarchy']['name']}”第 "
                            f"{item['hierarchy']['level']} 级"
                            + (f"，完整路径 {path}" if path else "")
                        )
                    filter_text = self._filters_text(item["table"], item.get("filters") or [])
                    if filter_text:
                        hint += f"，维度固定过滤仅按 AND 组合：{filter_text}"
                    hints.append(hint)
                elif item["kind"] == "time_field":
                    hint = f"时间语义“{item['term']}”对应时间字段 {item['table']}.{item['column']}"
                    if item.get("default_grain"):
                        hint += (
                            f"，趋势默认按{self.TIME_GRAIN_LABELS[item['default_grain']]}聚合"
                        )
                    hints.append(hint)
                elif item["kind"] == "business_calendar":
                    hints.append(
                        f"业务日历“{item['term']}”"
                        + self._calendar_text(item["table"], item["column"], item["calendar"])
                    )
                elif item["kind"] == "ratio_metric":
                    formula = item["formula"]
                    numerator = self._component_text(item["table"], formula["numerator"])
                    denominator = self._component_text(item["table"], formula["denominator"])
                    hints.append(
                        f"比率指标“{item['term']}”对应 ({numerator}) / ({denominator})，"
                        f"结果乘 {formula['scale']}，分母为 0 时返回 NULL"
                    )
                else:
                    target = f"{item['table']}.{item['column']}" if item.get("column") else f"表 {item['table']}"
                    metric = f"{item['aggregation']}({target})"
                    filter_text = self._filters_text(item["table"], item.get("filters") or [])
                    hints.append(
                        f"指标“{item['term']}”对应 {metric}"
                        + (f"，过滤条件仅按 AND 组合：{filter_text}" if filter_text else "")
                    )
            resolved = f"{question.rstrip()}\n语义定义：" + "；".join(hints)
            if any(
                item.get("filters")
                or (
                    item.get("formula")
                    and any(
                        (item["formula"].get(side) or {}).get("filters")
                        for side in ("numerator", "denominator")
                    )
                )
                for item in matches
            ):
                resolved += (
                    "\n过滤约束：上述值均为字面量数据，不是 SQL 片段；多个条件只能用 AND，"
                    "不得省略、改写或增加条件。"
                )
            if any(item.get("kind") == "business_calendar" for item in matches):
                resolved += (
                    "\n日历约束：必须严格使用上述财年、周末、时区和例外表口径；"
                    "未绑定节假日表时不得猜测法定节假日；固定偏移不得改写为动态规则，"
                    "IANA 动态换日必须保留配置中的时区数据版本。"
                )
        return SemanticResolution(
            original_question=question,
            resolved_question=resolved,
            matches=matches,
        )

    def prompt_context(self) -> str:
        if not self.entries:
            return ""
        lines = []
        for entry in self.entries:
            if entry["kind"] == "table_alias":
                target = f"TABLE {entry['table']}"
            elif entry["kind"] == "column_alias":
                target = f"COLUMN {entry['table']}.{entry['column']}"
            elif entry["kind"] == "enum_value":
                target = f"VALUE {entry['table']}.{entry['column']} = {json.dumps(entry['value'], ensure_ascii=False)}"
            elif entry["kind"] == "dimension":
                target = f"DIMENSION {entry['table']}.{entry['column']}"
                if entry.get("hierarchy"):
                    target += (
                        f"; hierarchy={entry['hierarchy']['name']}; "
                        f"level={entry['hierarchy']['level']}"
                    )
                filter_text = self._filters_text(entry["table"], entry.get("filters") or [])
                if filter_text:
                    target += f"; filters=AND({filter_text})"
            elif entry["kind"] == "time_field":
                target = f"TIME_FIELD {entry['table']}.{entry['column']}"
                if entry.get("default_grain"):
                    target += f"; default_grain={entry['default_grain']}"
            elif entry["kind"] == "business_calendar":
                calendar = entry["calendar"]
                target = (
                    f"BUSINESS_CALENDAR target={entry['table']}.{entry['column']}; "
                    f"fiscal_year_start={calendar['fiscal_year_start_month']:02d}-"
                    f"{calendar['fiscal_year_start_day']:02d}; timezone={calendar['timezone']}; "
                    f"fiscal_year_label={calendar['fiscal_year_label']}; "
                    f"fiscal_year_label_source={calendar['fiscal_year_label_source']}; "
                    f"storage_basis={calendar['storage_basis']}; "
                    f"storage_basis_source={calendar['storage_basis_source']}; "
                    f"timezone_conversion={calendar['timezone_conversion']}; "
                    f"week_start_iso={calendar['week_start']}; weekend_iso={calendar['weekend_days']}"
                )
                if calendar.get("business_utc_offset_minutes") is not None:
                    target += (
                        f"; business_utc_offset_minutes="
                        f"{calendar['business_utc_offset_minutes']}; fixed_offset_only=true"
                    )
                if calendar.get("timezone_conversion") == "iana_tzdata":
                    target += (
                        f"; tzdata_version={calendar['tzdata_version']}"
                        f"; iana_version={calendar['iana_version']}"
                    )
                if calendar.get("holiday_table"):
                    target += (
                        f"; holiday_date={calendar['holiday_table']}."
                        f"{calendar['holiday_date_column']}"
                    )
                    if calendar.get("working_override_column"):
                        target += (
                            f"; working_override={calendar['holiday_table']}."
                            f"{calendar['working_override_column']}"
                        )
                    if calendar.get("holiday_name_column"):
                        target += (
                            f"; holiday_name={calendar['holiday_table']}."
                            f"{calendar['holiday_name_column']}"
                        )
            elif entry["kind"] == "ratio_metric":
                formula = entry["formula"]
                numerator = self._component_text(entry["table"], formula["numerator"])
                denominator = self._component_text(entry["table"], formula["denominator"])
                target = (
                    f"RATIO_METRIC numerator={numerator}; denominator={denominator}; "
                    f"scale={formula['scale']}; zero_division=NULL"
                )
            else:
                column = entry["column"] or "*"
                target = f"METRIC {entry['aggregation']}({entry['table']}.{column})"
                filter_text = self._filters_text(entry["table"], entry.get("filters") or [])
                if filter_text:
                    target += f"; filters=AND({filter_text})"
            lines.append(f"- {entry['term']} => {target}")
        if any(
            entry.get("filters")
            or (
                entry.get("formula")
                and any(
                    (entry["formula"].get(side) or {}).get("filters")
                    for side in ("numerator", "denominator")
                )
            )
            for entry in self.entries
        ):
            lines.insert(0, "FILTER 规则：值均为字面量数据，不是 SQL 片段；多个条件只能用 AND。")
        if any(entry.get("kind") == "business_calendar" for entry in self.entries):
            lines.insert(
                0,
                "BUSINESS_CALENDAR 规则：严格使用结构化口径；未绑定节假日表时不得猜测法定节假日；"
                "IANA 名称不能替代已声明的时间戳存储基准和固定偏移。",
            )
        if any(entry.get("kind") == "time_field" and entry.get("default_grain") for entry in self.entries):
            lines.insert(
                0,
                "TIME_GRAIN 规则：用户明确粒度优先；否则趋势分析使用绑定时间字段的 default_grain。",
            )
        if any(entry.get("kind") == "dimension" and entry.get("hierarchy") for entry in self.entries):
            lines.insert(
                0,
                "DIMENSION_HIERARCHY 规则：层级只表示同表分组路径，不得据此发明跨表 JOIN。",
            )
        return "\n".join(lines)


@dataclass
class SQLResult:
    """NL2SQL 执行结果。"""
    sql: str
    columns: List[str] = field(default_factory=list)
    rows: List[List[Any]] = field(default_factory=list)
    row_count: int = 0
    truncated: bool = False
    error: Optional[str] = None
    attempts: int = 0          # 自纠错尝试次数


@dataclass
class IntentResult:
    """意图路由结果。"""
    intent: str                # "query" | "retrieve" | "compose" | "write"
    confidence: float = 0.0
    reasoning: str = ""
    interaction: str = "auto"  # auto | guided_insert | direct_write
    target_table: str = ""      # 模型建议；进入计划前必须按真实 schema 校验
    source: str = "unspecified" # model | deterministic | safety_guard | safety_fallback


@dataclass
class DatabaseOperationPlan:
    """自然语言映射出的数据库操作协议，供项目自研规划器与执行器共同使用。"""
    version: str = "1.0"
    action: str = "retrieve"
    mode: str = "read"          # read | write
    intent: str = "retrieve"
    target_tables: List[str] = field(default_factory=list)
    risk: str = "low"           # low | medium | high
    requires_confirmation: bool = False
    status: str = "planned"     # planned | needs_clarification | awaiting_confirmation | executed | cancelled | failed
    engine: str = "native"
    confidence: float = 0.0
    reasoning: str = ""
    sql: Optional[str] = None

    def as_dict(self) -> dict:
        """稳定的 JSON 结构，避免前端或其他本地组件依赖内部对象。"""
        return {
            "version": self.version,
            "action": self.action,
            "mode": self.mode,
            "intent": self.intent,
            "target_tables": list(self.target_tables),
            "risk": self.risk,
            "requires_confirmation": self.requires_confirmation,
            "status": self.status,
            "engine": self.engine,
            "confidence": round(float(self.confidence), 2),
            "reasoning": self.reasoning,
            "sql": self.sql,
        }


@dataclass
class RagResult:
    """RAG 检索问答结果。"""
    answer: str
    evidence: List[dict] = field(default_factory=list)  # 召回的片段/行


OPERATION_GRAPH_CONTRACTS = {
    "inspect_relations": {
        "input": {"type": "table_set", "required": ["tables"]},
        "output": {
            "type": "relation_check",
            "required": ["summary", "tables", "connected", "edges", "paths"],
        },
    },
    "query": {
        "input": {"type": "natural_language_query", "required": ["question"]},
        "output": {"type": "tabular_query_result", "required": ["summary", "rows", "sql"]},
    },
    "retrieve": {
        "input": {"type": "natural_language_query", "required": ["question"]},
        "output": {"type": "evidence_result", "required": ["summary", "evidence"]},
    },
    "synthesize": {
        "input": {"type": "upstream_results", "required": ["dependencies"]},
        "output": {"type": "composed_answer", "required": ["summary", "sources"]},
    },
}


@dataclass
class OperationGraphNode:
    """自研操作图节点；工具、依赖和失败策略都必须显式声明。"""
    node_id: str
    tool: str                  # inspect_relations | query | retrieve | synthesize
    input: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    failure_policy: str = "continue"  # continue | stop
    input_contract: Dict[str, Any] = field(default_factory=dict)
    output_contract: Dict[str, Any] = field(default_factory=dict)
    status: str = "pending"           # pending | running | completed | failed | skipped
    output: Optional[dict] = None
    error: Optional[str] = None

    def __post_init__(self) -> None:
        defaults = OPERATION_GRAPH_CONTRACTS.get(self.tool) or {}
        if not self.input_contract and defaults.get("input"):
            self.input_contract = {
                "type": defaults["input"]["type"],
                "required": list(defaults["input"]["required"]),
            }
        if not self.output_contract and defaults.get("output"):
            self.output_contract = {
                "type": defaults["output"]["type"],
                "required": list(defaults["output"]["required"]),
            }

    def as_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "tool": self.tool,
            "input": self.input,
            "parameters": dict(self.parameters),
            "depends_on": list(self.depends_on),
            "failure_policy": self.failure_policy,
            "input_contract": dict(self.input_contract),
            "output_contract": dict(self.output_contract),
            "status": self.status,
            "output": self.output,
            "error": self.error,
        }


@dataclass
class OperationGraph:
    """有向无环操作图协议；当前仅承载可安全降级的只读组合分析。"""
    objective: str
    nodes: List[OperationGraphNode] = field(default_factory=list)
    version: str = "3.1"
    graph_id: str = field(default_factory=lambda: f"graph-{uuid.uuid4().hex[:12]}")
    strategy: str = "deterministic"
    target_tables: List[str] = field(default_factory=list)
    status: str = "planned"           # planned | running | completed | partial | failed
    error: Optional[str] = None

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "graph_id": self.graph_id,
            "objective": self.objective,
            "strategy": self.strategy,
            "target_tables": list(self.target_tables),
            "status": self.status,
            "error": self.error,
            "nodes": [node.as_dict() for node in self.nodes],
        }


@dataclass
class QueryIntentContract:
    """Bounded semantic contract emitted by the post-generation query reviewer."""
    outputs: List[str] = field(default_factory=list)
    row_grain: str = "unknown"
    filters: List[str] = field(default_factory=list)
    grouping: List[str] = field(default_factory=list)
    relations: List[str] = field(default_factory=list)
    ordering_limit: str = ""
    version: str = "1.0"

    _ROW_GRAINS = {
        "single_value", "single_row", "detail_rows", "one_row_per_entity",
        "one_row_per_group", "top_k", "set_of_entities", "unknown",
    }

    @staticmethod
    def _bounded_items(raw: Any, limit: int = 12) -> List[str]:
        if not isinstance(raw, list):
            return []
        out: List[str] = []
        for item in raw:
            text = re.sub(r"\s+", " ", str(item or "")).strip()[:240]
            if text and text not in out:
                out.append(text)
            if len(out) >= limit:
                break
        return out

    @classmethod
    def from_payload(cls, raw: Any) -> "QueryIntentContract":
        payload = raw if isinstance(raw, dict) else {}
        grain = str(payload.get("row_grain") or "unknown").strip().lower()
        if grain not in cls._ROW_GRAINS:
            grain = "unknown"
        return cls(
            outputs=cls._bounded_items(payload.get("outputs")),
            row_grain=grain,
            filters=cls._bounded_items(payload.get("filters")),
            grouping=cls._bounded_items(payload.get("grouping")),
            relations=cls._bounded_items(payload.get("relations")),
            ordering_limit=re.sub(
                r"\s+", " ", str(payload.get("ordering_limit") or ""),
            ).strip()[:240],
        )

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "outputs": list(self.outputs),
            "row_grain": self.row_grain,
            "filters": list(self.filters),
            "grouping": list(self.grouping),
            "relations": list(self.relations),
            "ordering_limit": self.ordering_limit,
        }

    def is_declared(self) -> bool:
        return bool(
            self.outputs or self.filters or self.grouping or self.relations
            or self.ordering_limit or self.row_grain != "unknown"
        )


@dataclass
class RelationalAlgebraContract:
    """Question/schema-derived relational constraints independent of model intent.

    ``QueryIntentContract`` is useful diagnostics, but it is emitted by the same
    model that writes the SQL. This contract is compiled locally from bounded,
    high-confidence language patterns, physical schema facts and explicit
    business-dictionary mappings. It therefore provides an independent check
    without attempting to be a complete SQL parser or silently rewriting SQL.
    """
    required_operators: List[str] = field(default_factory=list)
    output_columns: List[str] = field(default_factory=list)
    output_bindings: List[dict] = field(default_factory=list)
    output_layout: List[dict] = field(default_factory=list)
    result_grain: dict = field(default_factory=dict)
    output_bundles: List[List[str]] = field(default_factory=list)
    modifier_filters: List[str] = field(default_factory=list)
    grouping_keys: List[str] = field(default_factory=list)
    aggregate_requirements: List[dict] = field(default_factory=list)
    relationship_thresholds: List[dict] = field(default_factory=list)
    filter_requirements: List[dict] = field(default_factory=list)
    ordering_requirements: List[dict] = field(default_factory=list)
    set_requirements: List[dict] = field(default_factory=list)
    distinct_count_requirements: List[dict] = field(default_factory=list)
    relation_paths: List[dict] = field(default_factory=list)
    aggregation_stages: List[dict] = field(default_factory=list)
    aggregate_subjects: List[dict] = field(default_factory=list)
    value_domain_requirements: List[dict] = field(default_factory=list)
    ratio_requirements: List[dict] = field(default_factory=list)
    correlation_requirements: List[dict] = field(default_factory=list)
    predicate_literal_policies: List[dict] = field(default_factory=list)
    distinct_row_requirements: List[dict] = field(default_factory=list)
    boolean_filter_requirements: List[dict] = field(default_factory=list)
    ambiguities: List[dict] = field(default_factory=list)
    comparison_quantifier: str = ""
    comparison_direction: str = ""
    tie_policy: str = ""
    tie_breaker_columns: List[str] = field(default_factory=list)
    evidence: List[str] = field(default_factory=list)
    version: str = "1.10"

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "required_operators": list(self.required_operators),
            "output_columns": list(self.output_columns),
            "output_bindings": [dict(item) for item in self.output_bindings],
            "output_layout": [dict(item) for item in self.output_layout],
            "result_grain": dict(self.result_grain),
            "output_bundles": [list(items) for items in self.output_bundles],
            "modifier_filters": list(self.modifier_filters),
            "grouping_keys": list(self.grouping_keys),
            "aggregate_requirements": [dict(item) for item in self.aggregate_requirements],
            "relationship_thresholds": [
                dict(item) for item in self.relationship_thresholds
            ],
            "filter_requirements": [
                dict(item) for item in self.filter_requirements
            ],
            "ordering_requirements": [
                dict(item) for item in self.ordering_requirements
            ],
            "set_requirements": [dict(item) for item in self.set_requirements],
            "distinct_count_requirements": [
                dict(item) for item in self.distinct_count_requirements
            ],
            "relation_paths": [dict(item) for item in self.relation_paths],
            "aggregation_stages": [dict(item) for item in self.aggregation_stages],
            "aggregate_subjects": [
                dict(item) for item in self.aggregate_subjects
            ],
            "value_domain_requirements": [
                dict(item) for item in self.value_domain_requirements
            ],
            "ratio_requirements": [dict(item) for item in self.ratio_requirements],
            "correlation_requirements": [
                dict(item) for item in self.correlation_requirements
            ],
            "predicate_literal_policies": [
                dict(item) for item in self.predicate_literal_policies
            ],
            "distinct_row_requirements": [
                dict(item) for item in self.distinct_row_requirements
            ],
            "boolean_filter_requirements": [
                dict(item) for item in self.boolean_filter_requirements
            ],
            "ambiguities": [dict(item) for item in self.ambiguities],
            "comparison_quantifier": self.comparison_quantifier,
            "comparison_direction": self.comparison_direction,
            "tie_policy": self.tie_policy,
            "tie_breaker_columns": list(self.tie_breaker_columns),
            "evidence": list(self.evidence),
        }

    def is_declared(self) -> bool:
        return bool(
            self.required_operators or self.output_columns or self.output_bindings
            or self.output_layout or self.result_grain
            or self.output_bundles
            or self.modifier_filters or self.grouping_keys
            or self.aggregate_requirements or self.relationship_thresholds
            or self.filter_requirements or self.ordering_requirements
            or self.set_requirements
            or self.distinct_count_requirements
            or self.relation_paths or self.aggregation_stages
            or self.aggregate_subjects
            or self.value_domain_requirements
            or self.ratio_requirements or self.correlation_requirements
            or self.predicate_literal_policies
            or self.distinct_row_requirements
            or self.boolean_filter_requirements or self.ambiguities
            or self.comparison_quantifier or self.tie_policy
            or self.tie_breaker_columns
        )

    def is_actionable(self) -> bool:
        return bool(
            self.required_operators or self.output_columns or self.output_bindings
            or self.output_layout or self.result_grain
            or self.output_bundles
            or self.modifier_filters or self.grouping_keys
            or self.aggregate_requirements or self.relationship_thresholds
            or self.filter_requirements or self.ordering_requirements
            or self.set_requirements
            or self.distinct_count_requirements
            or self.relation_paths or self.aggregation_stages
            or self.aggregate_subjects
            or self.value_domain_requirements
            or self.ratio_requirements or self.correlation_requirements
            or self.predicate_literal_policies
            or self.distinct_row_requirements
            or self.boolean_filter_requirements or self.ambiguities
            or self.comparison_quantifier
            or self.tie_breaker_columns
            or self.tie_policy in {"all_ties", "single_row"}
        )


@dataclass(frozen=True)
class RelationalColumnRef:
    """Schema-validated physical column used by a deterministic query plan."""
    table: str
    column: str

    def as_dict(self) -> dict:
        return {"table": self.table, "column": self.column}


@dataclass(frozen=True)
class RelationalJoinEdge:
    """One declared physical FK edge; direction does not imply join direction."""
    left: RelationalColumnRef
    right: RelationalColumnRef
    source: str = "foreign_key"
    join_type: str = "INNER"

    def as_dict(self) -> dict:
        return {
            "left": self.left.as_dict(),
            "right": self.right.as_dict(),
            "source": self.source,
            "join_type": self.join_type,
        }


@dataclass(frozen=True)
class RelationalAggregate:
    """Bounded aggregate node supported by the native relational compiler."""
    function: str
    source_table: str
    column: str = "*"
    alias: str = "__dbagent_measure"
    distinct: bool = False

    def as_dict(self) -> dict:
        return {
            "function": self.function,
            "source_table": self.source_table,
            "column": self.column,
            "alias": self.alias,
            "distinct": self.distinct,
        }


@dataclass(frozen=True)
class RelationalFilterPredicate:
    """Schema-bound scalar predicate; values remain typed data, never SQL."""
    column: RelationalColumnRef
    operator: str
    value: Any
    value_type: str

    def as_dict(self) -> dict:
        return {
            "column": self.column.as_dict(),
            "operator": self.operator,
            "value": self.value,
            "value_type": self.value_type,
        }


@dataclass(frozen=True)
class RelationalRanking:
    """Ordering/reduction node applied after grouped aggregation."""
    direction: str
    tie_policy: str
    limit: Optional[int] = None

    def as_dict(self) -> dict:
        return {
            "direction": self.direction,
            "tie_policy": self.tie_policy,
            "limit": self.limit,
        }


@dataclass(frozen=True)
class RelationalQueryPlan:
    """Executable, model-independent relational plan for a proven query shape.

    This is intentionally smaller than SQL.  Every source, projection, group
    key and join edge is a physical schema reference, while aggregation and
    ranking are closed enums.  A dialect renderer, not the language model,
    turns the validated plan into SQL.
    """
    sources: List[str]
    joins: List[RelationalJoinEdge]
    projections: List[RelationalColumnRef]
    group_keys: List[RelationalColumnRef]
    aggregate: RelationalAggregate
    ranking: RelationalRanking
    contract_version: str
    filters: List[RelationalFilterPredicate] = field(default_factory=list)
    evidence: List[str] = field(default_factory=list)
    dialect: str = "sqlite"
    version: str = "1.1"

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "dialect": self.dialect,
            "sources": list(self.sources),
            "joins": [item.as_dict() for item in self.joins],
            "projections": [item.as_dict() for item in self.projections],
            "group_keys": [item.as_dict() for item in self.group_keys],
            "aggregate": self.aggregate.as_dict(),
            "ranking": self.ranking.as_dict(),
            "filters": [item.as_dict() for item in self.filters],
            "contract_version": self.contract_version,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True)
class RelationalGroupedAggregatePlan:
    """Typed entity projection plus one grouped relationship count.

    This plan is deliberately separate from ``RelationalQueryPlan``: returning
    the aggregate is a different operation from using it only to rank entity
    rows.  Keeping the shapes distinct prevents a renderer or validator from
    silently dropping the requested count column.
    """
    sources: List[str]
    joins: List[RelationalJoinEdge]
    projections: List[RelationalColumnRef]
    group_keys: List[RelationalColumnRef]
    aggregate: RelationalAggregate
    contract_version: str
    include_zero: bool = True
    filters: List[RelationalFilterPredicate] = field(default_factory=list)
    evidence: List[str] = field(default_factory=list)
    dialect: str = "sqlite"
    version: str = "1.0"

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "dialect": self.dialect,
            "kind": "grouped_aggregate",
            "sources": list(self.sources),
            "joins": [item.as_dict() for item in self.joins],
            "projections": [item.as_dict() for item in self.projections],
            "group_keys": [item.as_dict() for item in self.group_keys],
            "aggregate": self.aggregate.as_dict(),
            "include_zero": self.include_zero,
            "filters": [item.as_dict() for item in self.filters],
            "contract_version": self.contract_version,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True)
class RelationalOrderTerm:
    """One schema-bound ordering term for a grouped metrics plan."""
    direction: str
    column: Optional[RelationalColumnRef] = None
    aggregate_alias: str = ""

    def as_dict(self) -> dict:
        return {
            "direction": self.direction,
            "column": self.column.as_dict() if self.column else None,
            "aggregate_alias": self.aggregate_alias,
        }


@dataclass(frozen=True)
class RelationalGroupedMetricsPlan:
    """Typed GROUP BY plan with one dimension and two to six aggregates.

    Unlike semantic-catalog metrics, this plan is compiled directly from exact
    physical column mentions, declared/user-explicit relations and bounded
    aggregate language.  It therefore covers ordinary ad-hoc grouped metrics
    without asking the model to own projection, grain or denominator choices.
    """
    sources: List[str]
    joins: List[RelationalJoinEdge]
    dimensions: List[RelationalColumnRef]
    group_keys: List[RelationalColumnRef]
    aggregates: List[RelationalAggregate]
    filters: List[RelationalFilterPredicate]
    order_by: List[RelationalOrderTerm]
    contract_version: str
    evidence: List[str] = field(default_factory=list)
    dialect: str = "sqlite"
    version: str = "1.0"

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "dialect": self.dialect,
            "kind": "grouped_metrics",
            "sources": list(self.sources),
            "joins": [item.as_dict() for item in self.joins],
            "dimensions": [item.as_dict() for item in self.dimensions],
            "group_keys": [item.as_dict() for item in self.group_keys],
            "aggregates": [item.as_dict() for item in self.aggregates],
            "filters": [item.as_dict() for item in self.filters],
            "order_by": [item.as_dict() for item in self.order_by],
            "contract_version": self.contract_version,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True)
class RelationalSetBranch:
    """One independently scoped projection participating in a set operation."""
    source: str
    projection: RelationalColumnRef
    filters: List[RelationalFilterPredicate] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "source": self.source,
            "projection": self.projection.as_dict(),
            "filters": [item.as_dict() for item in self.filters],
        }


@dataclass(frozen=True)
class RelationalSetQueryPlan:
    """Typed independent set operation; it never invents a JOIN relation."""
    operator: str
    branches: List[RelationalSetBranch]
    output_name: str
    contract_version: str
    proof_edges: List[RelationalJoinEdge] = field(default_factory=list)
    evidence: List[str] = field(default_factory=list)
    dialect: str = "sqlite"
    version: str = "1.0"

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "dialect": self.dialect,
            "kind": "set_operation",
            "operator": self.operator,
            "branches": [item.as_dict() for item in self.branches],
            "output_name": self.output_name,
            "proof_edges": [item.as_dict() for item in self.proof_edges],
            "contract_version": self.contract_version,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True)
class RelationalScalarAggregatePlan:
    """Typed scalar aggregate, initially used for proven distinct entity counts."""
    source: str
    aggregate: RelationalAggregate
    output_name: str
    proof_edges: List[RelationalJoinEdge]
    contract_version: str
    filters: List[RelationalFilterPredicate] = field(default_factory=list)
    evidence: List[str] = field(default_factory=list)
    dialect: str = "sqlite"
    version: str = "1.0"

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "dialect": self.dialect,
            "kind": "scalar_aggregate",
            "source": self.source,
            "aggregate": self.aggregate.as_dict(),
            "output_name": self.output_name,
            "proof_edges": [item.as_dict() for item in self.proof_edges],
            "filters": [item.as_dict() for item in self.filters],
            "contract_version": self.contract_version,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True)
class RelationalScalarRankingPlan:
    """Typed arg-min/arg-max over a physical scalar after proven joins/filters."""
    sources: List[str]
    joins: List[RelationalJoinEdge]
    projections: List[RelationalColumnRef]
    filters: List[RelationalFilterPredicate]
    order_by: RelationalColumnRef
    direction: str
    limit: int
    tie_breakers: List[RelationalColumnRef]
    contract_version: str
    evidence: List[str] = field(default_factory=list)
    dialect: str = "sqlite"
    version: str = "1.0"

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "dialect": self.dialect,
            "kind": "scalar_ranking",
            "sources": list(self.sources),
            "joins": [item.as_dict() for item in self.joins],
            "projections": [item.as_dict() for item in self.projections],
            "filters": [item.as_dict() for item in self.filters],
            "order_by": self.order_by.as_dict(),
            "direction": self.direction,
            "limit": self.limit,
            "tie_breakers": [item.as_dict() for item in self.tie_breakers],
            "contract_version": self.contract_version,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True)
class QuerySemanticConflict:
    """Typed local rejection evidence used to condition candidate search."""
    code: str
    message: str
    constraints: dict = field(default_factory=dict)
    version: str = "1.0"

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "code": self.code,
            "message": self.message,
            "constraints": dict(self.constraints),
        }


@dataclass
class DBAnswer:
    """统一回答载体（bridge / WS / 前端用）。"""
    kind: str                  # "conversation" | "clarification" | "schema" | "query" | "retrieve" | "compose" | "error" | "write_form" | "write_pending" | "write_result"
    narrative: str             # 自然语言回答
    sql: Optional[str] = None
    columns: List[str] = field(default_factory=list)
    rows: List[List[Any]] = field(default_factory=list)
    datasets: List[dict] = field(default_factory=list)  # 多查询分支结果；单查询保持为空
    evidence: List[dict] = field(default_factory=list)
    steps: List[dict] = field(default_factory=list)
    error: Optional[str] = None
    confirm_id: Optional[str] = None   # 写提案确认ID（write_pending 时返回）
    write: Optional[dict] = None       # 写提案内容（kind/table/summary/preview/dangerous，write_pending 时返回）
    operation: Optional[dict] = None   # NL-to-Database 操作计划（动作/目标/风险/状态）
    clarification: Optional[dict] = None  # 歧义澄清请求（缺失项/候选/补充模板）
    graph: Optional[dict] = None       # 自研只读操作图（节点/依赖/状态/失败策略）
    semantic: Optional[dict] = None    # 业务术语到 schema 的结构化解析结果
    calendar_plan: Optional[dict] = None  # 确定性业务日历日期过滤计划
    metric_plan: Optional[dict] = None  # 确定性同表并列指标聚合计划
    dimension_plan: Optional[dict] = None  # 确定性同表维度聚合/下钻计划
    trend_plan: Optional[dict] = None  # 确定性单表时间趋势聚合计划
    relational_plan: Optional[dict] = None  # 本地类型化关系计划与确定性 SQL 编译结果


# ---------------------------------------------------------------------------
# 异常体系（分层，便于 bridge 转 HTTP 状态码）
# ---------------------------------------------------------------------------

class DBAgentError(Exception):
    """DB Agent 基异常。"""
    status_code = 500


class LLMServiceError(DBAgentError):
    """LLM 通道已完成内部重试后返回的终态服务错误。"""
    status_code = 502


class SchemaDiscoveryError(DBAgentError):
    status_code = 500


class SQLSecurityError(DBAgentError):
    """SQL 安全拦截（写操作/多语句/危险函数等）。"""
    status_code = 400


class NL2SQLError(DBAgentError):
    status_code = 500


class IntentRouterError(DBAgentError):
    status_code = 500


class RAGError(DBAgentError):
    status_code = 500


class OrchestratorError(DBAgentError):
    status_code = 500


# ---------------------------------------------------------------------------
# DBConnector —— 只读 sqlite 连接管理（安全底线：物理禁写）
# ---------------------------------------------------------------------------

class DBConnector:
    """只读连接工厂：uri mode=ro + PRAGMA query_only=ON，物理禁止任何写操作。"""

    def __init__(self, db_path: str):
        self.db_path = str(Path(db_path).resolve())

    def connect(self) -> sqlite3.Connection:
        """打开只读连接（每次调用返回新连接，调用方负责 close）。"""
        if not os.path.isfile(self.db_path):
            raise SchemaDiscoveryError(f"数据库文件不存在: {self.db_path}")
        try:
            conn = sqlite3.connect(
                f"file:{self.db_path}?mode=ro",
                uri=True,
                timeout=10.0,
                check_same_thread=False,
            )
            conn.execute("PRAGMA query_only = ON")
            conn.execute("PRAGMA busy_timeout = 10000")
            TimezoneRuntime.register_sqlite(conn)
            return conn
        except sqlite3.Error as e:
            raise SchemaDiscoveryError(f"打开只读连接失败: {e}") from e

    def connect_rw(self) -> sqlite3.Connection:
        """打开可写连接（仅用户确认后的写执行使用）。

        日常查询路径一律走只读 connect()；此方法只被 WritePreviewer
        （dry-run 预览，事务内执行后回滚）与 confirm_write（用户批准后
        正式执行）调用。
        """
        if not os.path.isfile(self.db_path):
            raise SchemaDiscoveryError(f"数据库文件不存在: {self.db_path}")
        try:
            conn = sqlite3.connect(
                self.db_path,
                timeout=10.0,
                check_same_thread=False,
            )
            conn.execute("PRAGMA busy_timeout = 5000")
            return conn
        except sqlite3.Error as e:
            raise SchemaDiscoveryError(f"打开可写连接失败: {e}") from e

    def close(self, conn: Optional[sqlite3.Connection]) -> None:
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass

    @staticmethod
    def interrupt(conn: Optional[sqlite3.Connection]) -> None:
        if conn is not None:
            try:
                conn.interrupt()
            except sqlite3.Error:
                pass


class RemoteDBConnector:
    """远程数据库只读连接工厂：mysql(pymysql) / postgresql(psycopg2)。

    与 DBConnector 同接口（connect/close/db_path/dialect），供 SchemaDiscovery/
    SQLSecurity 无差别使用；只读由连接参数保证（PG read_only、MySQL 连接用户权限）。
    """

    def __init__(self, cfg: dict):
        self.cfg = dict(cfg or {})
        self.dialect = (self.cfg.get("dialect") or "mysql").lower()
        if self.dialect not in ("mysql", "postgresql"):
            raise ValueError(f"unsupported dialect: {self.dialect}")
        self.db_path = (
            f"{self.dialect}://{self.cfg.get('host', '')}:{self.cfg.get('port', '')}"
            f"/{self.cfg.get('database', '')}"
        )

    def connect(self):
        if self.dialect == "mysql":
            import pymysql  # 函数级 import（标准库级可用，模块顶层勿 import）
            conn = pymysql.connect(
                host=self.cfg.get("host") or "127.0.0.1",
                port=int(self.cfg.get("port") or 3306),
                user=self.cfg.get("user") or "",
                password=self.cfg.get("password") or "",
                database=self.cfg.get("database") or "",
                connect_timeout=5, read_timeout=15, charset="utf8mb4",
                autocommit=True,
            )
            try:  # 尽力只读会话（老版本无此变量则静默）
                cur = conn.cursor()
                cur.execute("SET SESSION transaction_read_only = 1")
                cur.close()
            except Exception:
                pass
            return conn
        import psycopg2  # noqa: PLC0415
        return psycopg2.connect(
            host=self.cfg.get("host") or "127.0.0.1",
            port=int(self.cfg.get("port") or 5432),
            user=self.cfg.get("user") or "",
            password=self.cfg.get("password") or "",
            dbname=self.cfg.get("database") or "",
            connect_timeout=5,
            options="-c default_transaction_read_only=on -c statement_timeout=15000",
        )

    def close(self, conn) -> None:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def interrupt(self, conn) -> None:
        """Best-effort vendor cancellation used by the local timeout gate."""
        if conn is None:
            return
        try:
            if self.dialect == "postgresql" and hasattr(conn, "cancel"):
                conn.cancel()
                return
            # PyMySQL has no public asynchronous cancel API.  Closing the
            # transport is the only operation that can wake a thread blocked
            # in a server response without granting the read-only user KILL.
            transport = getattr(conn, "_sock", None)
            if transport is not None:
                try:
                    transport.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                transport.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# SchemaDiscovery —— 读 sqlite_master → schema 快照 + L1 索引（步骤2填充）
# ---------------------------------------------------------------------------

class SchemaDiscovery:
    """读取库结构：表/列/类型/主外键/行数/值域抽样 → SchemaSnapshot。"""

    def __init__(
        self,
        connector: DBConnector,
        sample_rows: int = 5,
        allowed_tables: Optional[List[str]] = None,
        allowed_columns: Optional[Dict[str, List[str]]] = None,
        row_filters: Optional[Dict[str, List[dict]]] = None,
    ):
        self.connector = connector
        self.sample_rows = sample_rows
        self.allowed_tables = (
            frozenset(str(name).casefold() for name in allowed_tables)
            if allowed_tables is not None else None
        )
        self.allowed_columns = {
            str(table).casefold(): frozenset(str(column).casefold() for column in columns)
            for table, columns in (allowed_columns or {}).items()
        }
        if self.allowed_columns and self.allowed_tables is None:
            raise ValueError("字段级授权必须建立在显式表级授权之上")
        self.row_filters = _normalize_row_scope(row_filters)
        if self.row_filters and self.allowed_tables is None:
            raise ValueError("行级授权必须建立在显式表级授权之上")

    def discover(self) -> SchemaSnapshot:
        conn = self.connector.connect()
        try:
            if getattr(self.connector, "dialect", None) in ("mysql", "postgresql"):
                return self._discover_remote(conn)
            if self.row_filters:
                _prepare_sqlite_row_views(conn, self.row_filters, self.allowed_columns)
            cur = conn.cursor()
            # 1) 收集真实表 + 影子表（FTS 内部表 type='shadow'，老版本 SQLite 无此标记）
            rows = cur.execute(
                "SELECT name, type, sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' "
                "AND type IN ('table','virtual','shadow')"
            ).fetchall()
            real: Dict[str, str] = {}
            shadow: set = set()
            for name, typ, sql in rows:
                if typ == "shadow":
                    shadow.add(name)
                else:
                    real[name] = sql or ""
            # 老版本兜底：FTS 影子表按命名模式排除
            fts_names = [
                n for n, s in real.items()
                if s.strip().upper().startswith("CREATE VIRTUAL TABLE") and "FTS" in s.upper()
            ]
            for n in list(real):
                for fts in fts_names:
                    if n.startswith(fts + "_") and n[len(fts) + 1:] in (
                        "data", "docsize", "idx", "config", "content",
                    ):
                        real.pop(n, None)
                        break
            if self.allowed_tables is not None:
                real = {
                    name: sql for name, sql in real.items()
                    if name.casefold() in self.allowed_tables
                }

            snapshot = SchemaSnapshot(db_path=self.connector.db_path, generated_at=time.time())
            for tname in sorted(real):
                try:
                    table = self._discover_table(
                        conn, tname, real[tname],
                        self.allowed_columns.get(tname.casefold()),
                    )
                    if self.allowed_tables is not None or self.allowed_columns:
                        table.create_sql = ""
                        for column in table.columns:
                            if column.fk_table and self.allowed_tables is not None \
                                    and column.fk_table.casefold() not in self.allowed_tables:
                                column.fk_table = None
                                column.fk_column = None
                                continue
                            target_columns = self.allowed_columns.get(
                                str(column.fk_table or "").casefold(),
                            )
                            if target_columns is not None and str(
                                column.fk_column or "",
                            ).casefold() not in target_columns:
                                column.fk_table = None
                                column.fk_column = None
                    snapshot.tables[tname] = table
                except sqlite3.Error:
                    continue  # 单表失败不阻塞整体发现
            return snapshot
        finally:
            self.connector.close(conn)

    def _discover_remote(self, conn) -> SchemaSnapshot:
        """远程库通过 information_schema 发现表、列、PK/FK、行数和抽样。"""
        dialect = self.connector.dialect
        cur = conn.cursor()
        try:
            if dialect == "mysql":
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' "
                    "ORDER BY table_name"
                )
                col_sql = (
                    "SELECT column_name, data_type, is_nullable, column_key "
                    "FROM information_schema.columns WHERE table_schema = DATABASE() "
                    "AND table_name = %s ORDER BY ordinal_position"
                )
                fk_sql = (
                    "SELECT column_name, referenced_table_name, referenced_column_name "
                    "FROM information_schema.key_column_usage "
                    "WHERE table_schema = DATABASE() AND table_name = %s "
                    "AND referenced_table_name IS NOT NULL ORDER BY ordinal_position"
                )
                pk_sql = ""
            else:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
                    "ORDER BY table_name"
                )
                col_sql = (
                    "SELECT column_name, data_type, is_nullable, '' "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s "
                    "ORDER BY ordinal_position"
                )
                # information_schema hides constraints from a role that owns
                # only SELECT.  pg_catalog exposes structural metadata without
                # requiring write or ownership privileges.
                pk_sql = (
                    "SELECT a.attname FROM pg_catalog.pg_index i "
                    "JOIN pg_catalog.pg_class c ON c.oid = i.indrelid "
                    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                    "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid "
                    "AND a.attnum = ANY(i.indkey) "
                    "WHERE i.indisprimary AND n.nspname = 'public' "
                    "AND c.relname = %s ORDER BY a.attnum"
                )
                fk_sql = (
                    "SELECT src_att.attname, target.relname, target_att.attname "
                    "FROM pg_catalog.pg_constraint con "
                    "JOIN pg_catalog.pg_class source ON source.oid = con.conrelid "
                    "JOIN pg_catalog.pg_namespace source_ns "
                    "ON source_ns.oid = source.relnamespace "
                    "JOIN pg_catalog.pg_class target ON target.oid = con.confrelid "
                    "JOIN LATERAL unnest(con.conkey) WITH ORDINALITY "
                    "AS src_key(attnum, ord) ON TRUE "
                    "JOIN LATERAL unnest(con.confkey) WITH ORDINALITY "
                    "AS dst_key(attnum, ord) ON dst_key.ord = src_key.ord "
                    "JOIN pg_catalog.pg_attribute src_att "
                    "ON src_att.attrelid = source.oid "
                    "AND src_att.attnum = src_key.attnum "
                    "JOIN pg_catalog.pg_attribute target_att "
                    "ON target_att.attrelid = target.oid "
                    "AND target_att.attnum = dst_key.attnum "
                    "WHERE con.contype = 'f' AND source_ns.nspname = 'public' "
                    "AND source.relname = %s ORDER BY src_key.ord"
                )
            names = [str(row[0]) for row in cur.fetchall()]

            def q(name: str) -> str:
                if dialect == "mysql":
                    return "`" + name.replace("`", "``") + "`"
                return '"' + name.replace('"', '""') + '"'

            snapshot = SchemaSnapshot(
                db_path=self.connector.db_path,
                generated_at=time.time(),
            )
            for tname in names:
                try:
                    table = DBTable(name=tname, create_sql="")
                    cur.execute(col_sql, (tname,))
                    for cname, ctype, nullable, ckey in cur.fetchall():
                        table.columns.append(DBColumn(
                            name=str(cname),
                            type=str(ctype or "TEXT"),
                            nullable=(str(nullable).upper() == "YES"),
                            pk=(str(ckey).upper() == "PRI"),
                        ))
                    if pk_sql:
                        cur.execute(pk_sql, (tname,))
                        primary_keys = {str(row[0]) for row in cur.fetchall()}
                        for column in table.columns:
                            column.pk = column.name in primary_keys
                    cur.execute(f"SELECT COUNT(*) FROM {q(tname)}")
                    count_row = cur.fetchone()
                    table.row_count = int(count_row[0]) if count_row else 0
                    cur.execute(fk_sql, (tname,))
                    fk_by_column = {
                        str(column_name): (str(target_table), str(target_column))
                        for column_name, target_table, target_column in cur.fetchall()
                    }
                    for column in table.columns:
                        target = fk_by_column.get(column.name)
                        if target:
                            column.fk_table, column.fk_column = target
                    if self.sample_rows > 0:
                        for column in table.columns:
                            try:
                                cur.execute(
                                    f"SELECT DISTINCT {q(column.name)} FROM {q(tname)} "
                                    f"LIMIT {int(self.sample_rows)}"
                                )
                                values = [
                                    str(row[0])[:40] for row in cur.fetchall()
                                    if row[0] is not None
                                ]
                                column.sample_values = values[:self.sample_rows]
                            except Exception:
                                column.sample_values = []
                    snapshot.tables[tname] = table
                except Exception:
                    continue  # 单表失败不阻塞整体发现
            return snapshot
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def _discover_table(
        self,
        conn: sqlite3.Connection,
        tname: str,
        create_sql: str,
        allowed_columns: Optional[frozenset[str]] = None,
    ) -> DBTable:
        tbl = DBTable(name=tname, create_sql=create_sql[:500])
        # 列信息
        column_rows = conn.execute(f'PRAGMA main.table_info("{tname}")').fetchall()
        actual_columns = {str(row[1]).casefold() for row in column_rows}
        if allowed_columns is not None:
            missing = sorted(allowed_columns - actual_columns)
            if missing:
                raise ValueError(f"字段级授权包含表 {tname} 中不存在的字段")
        for cid, cname, ctype, notnull, dflt, pk in column_rows:
            if allowed_columns is not None and str(cname).casefold() not in allowed_columns:
                continue
            tbl.columns.append(DBColumn(
                name=cname,
                type=ctype or "TEXT",
                nullable=not notnull,
                pk=bool(pk),
                default_sql="" if dflt is None else str(dflt),
            ))
        # 外键：PRAGMA foreign_key_list 每行 (id, seq, table, from, to, on_update, on_delete, match)
        fk_map: Dict[str, tuple] = {}
        for _id, _seq, ftable, fcol, tocol, *_ in conn.execute(
            f'PRAGMA main.foreign_key_list("{tname}")'
        ):
            fk_map[fcol] = (ftable, tocol)
        for col in tbl.columns:
            if col.name in fk_map:
                col.fk_table, col.fk_column = fk_map[col.name]
        # 行数
        try:
            tbl.row_count = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
        except sqlite3.Error:
            tbl.row_count = -1
        # 值域抽样（BLOB 跳过；异常静默，保证发现不失败）
        if self.sample_rows > 0:
            for col in tbl.columns:
                if "BLOB" in col.type.upper():
                    continue
                try:
                    samples = conn.execute(
                        f'SELECT DISTINCT "{col.name}" FROM "{tname}" LIMIT ?',
                        (self.sample_rows,),
                    ).fetchall()
                    vals = []
                    for (v,) in samples:
                        if v is None:
                            continue
                        s = str(v)
                        vals.append(s[:40])
                    col.sample_values = vals[: self.sample_rows]
                except sqlite3.Error:
                    col.sample_values = []
        return tbl


# ---------------------------------------------------------------------------
# SQLSecurity —— 单语句校验 / 写操作拦截 / LIMIT 强制 / 超时（步骤3填充）
# ---------------------------------------------------------------------------

def _sql_code_only(
    sql: str, *, mask_identifiers: bool = True, mask_literals: bool = True,
) -> str:
    """用空格屏蔽字符串、引用标识符与注释，保留 SQL 代码位置。"""
    source = str(sql or "")
    output = list(source)
    index = 0
    quote = ""
    while index < len(source):
        char = source[index]
        next_char = source[index + 1] if index + 1 < len(source) else ""
        if quote:
            if len(quote) > 1:
                if source.startswith(quote, index):
                    for offset in range(len(quote)):
                        output[index + offset] = " "
                    index += len(quote)
                    quote = ""
                else:
                    output[index] = " "
                    index += 1
                continue
            if mask_identifiers or (quote == "'" and mask_literals) or len(quote) > 1:
                output[index] = " "
            if char == quote:
                if next_char == quote:
                    if mask_identifiers or (quote == "'" and mask_literals):
                        output[index + 1] = " "
                    index += 2
                    continue
                quote = ""
            index += 1
            continue
        if char == "-" and next_char == "-":
            output[index] = output[index + 1] = " "
            index += 2
            while index < len(source) and source[index] not in "\r\n":
                output[index] = " "
                index += 1
            continue
        if char == "#":
            output[index] = " "
            index += 1
            while index < len(source) and source[index] not in "\r\n":
                output[index] = " "
                index += 1
            continue
        if char == "/" and next_char == "*":
            output[index] = output[index + 1] = " "
            index += 2
            depth = 1
            while index < len(source) and depth:
                output[index] = " "
                following = source[index + 1] if index + 1 < len(source) else ""
                if source[index] == "/" and following == "*":
                    output[index + 1] = " "
                    depth += 1
                    index += 2
                elif source[index] == "*" and following == "/":
                    output[index + 1] = " "
                    depth -= 1
                    index += 2
                else:
                    index += 1
            continue
        if char == "$":
            delimiter = re.match(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$", source[index:])
            if delimiter:
                quote = delimiter.group(0)
                for offset in range(len(quote)):
                    output[index + offset] = " "
                index += len(quote)
                continue
        if char in {"'", '"', chr(96)}:
            quote = char
            if mask_identifiers or (char == "'" and mask_literals):
                output[index] = " "
        elif char == "[":
            quote = "]"
            if mask_identifiers:
                output[index] = " "
        index += 1
    return "".join(output)


def _normalize_single_sql_statement(sql: str, error_type, message: str) -> tuple[str, str]:
    """移除至多一个真实结尾分号，并拒绝其余代码分号。"""
    statement = str(sql or "").strip()
    if not statement:
        raise error_type("SQL 为空")
    executable_probe = _sql_code_only(statement.replace("/*!", "__MYSQL_EXECUTABLE_COMMENT__"))
    if "__MYSQL_EXECUTABLE_COMMENT__" in executable_probe:
        raise error_type("禁止 MySQL 可执行注释")
    code = _sql_code_only(statement)
    positions = [index for index, char in enumerate(code) if not char.isspace()]
    if positions and code[positions[-1]] == ";":
        terminator = positions[-1]
        statement = statement[:terminator] + statement[terminator + 1:]
        code = _sql_code_only(statement)
    if ";" in code:
        raise error_type(message)
    return statement.strip(), code


class OriginalSQLRequestGuard:
    """Reject executable multi-statement text before intent/model routing.

    Final generated SQL is still validated by ``SQLSecurity``.  This separate
    boundary prevents a model from silently selecting the harmless first
    statement from a user request that also contains a write statement.
    """

    _START_RE = re.compile(
        r"\b(?:SELECT|WITH|INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|"
        r"ATTACH|DETACH|PRAGMA|GRANT|REVOKE)\b",
        re.IGNORECASE,
    )
    _EXECUTION_RE = re.compile(
        r"(?:执行|运行|执行以下|跑一下|execute|run)\s*[:：]?",
        re.IGNORECASE,
    )

    @classmethod
    def reject_reason(cls, question: str) -> str:
        source = str(question or "").replace("；", ";")
        code = _sql_code_only(source, mask_identifiers=False, mask_literals=True)
        starts = list(cls._START_RE.finditer(code))
        if len(starts) < 2 or ";" not in code:
            return ""
        first = starts[0]
        has_statement_boundary = any(
            ";" in code[left.end():right.start()]
            for left, right in zip(starts, starts[1:])
        )
        prefix = code[:first.start()]
        starts_as_sql = not prefix.strip()
        explicitly_executable = bool(cls._EXECUTION_RE.search(prefix[-64:]))
        if has_statement_boundary and (starts_as_sql or explicitly_executable):
            return (
                "检测到原始请求包含多条可执行 SQL；为避免只执行其中一部分或混入写操作，"
                "本次请求未进入模型和数据库。请一次只提交一个数据库操作；写操作需走预览与确认。"
            )
        return ""


def _normalize_column_scope(
    allowed_columns: Optional[Dict[str, List[str]]],
) -> Dict[str, frozenset[str]]:
    return {
        str(table).casefold(): frozenset(str(column).casefold() for column in columns)
        for table, columns in (allowed_columns or {}).items()
    }


def _normalize_row_scope(row_filters: Optional[Dict[str, List[dict]]]) -> Dict[str, tuple[dict, ...]]:
    normalized: Dict[str, tuple[dict, ...]] = {}
    allowed_operators = {
        "eq", "neq", "gt", "gte", "lt", "lte", "in", "not_in",
        "is_null", "is_not_null",
    }
    for table, filters in (row_filters or {}).items():
        if not isinstance(filters, list) or not filters:
            raise ValueError("行级授权表必须提供非空过滤列表")
        items = []
        for item in filters:
            if not isinstance(item, dict):
                raise ValueError("行级过滤必须是结构化对象")
            column = str(item.get("column") or "").strip()
            operator = str(item.get("operator") or "").strip().lower()
            if not column or operator not in allowed_operators:
                raise ValueError("行级过滤字段或操作符无效")
            items.append({"column": column, "operator": operator, "value": item.get("value")})
        normalized[str(table).casefold()] = tuple(items)
    return normalized


def _sqlite_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("行级过滤数值必须有限")
        return repr(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise ValueError("行级过滤值类型不受支持")


def _create_sqlite_temp_view(conn: sqlite3.Connection, sql: str) -> None:
    """Create a connection-local view without weakening physical main-db RO."""
    query_only = bool(conn.execute("PRAGMA query_only").fetchone()[0])
    if query_only:
        conn.execute("PRAGMA query_only = OFF")
    try:
        conn.execute(sql)
    finally:
        if query_only:
            conn.execute("PRAGMA query_only = ON")


def _prepare_sqlite_row_views(
    conn: sqlite3.Connection,
    row_filters: Dict[str, tuple[dict, ...]],
    allowed_columns: Dict[str, frozenset[str]],
) -> Dict[str, frozenset[str]]:
    """Shadow row-scoped main tables with filtered TEMP views.

    The returned column sets are the only main-table columns the view itself
    may read. A later authorizer denies direct `main.table` access and only
    permits those reads when SQLite reports the TEMP view as their source.
    """
    internal_columns: Dict[str, frozenset[str]] = {}
    for folded_table, filters in row_filters.items():
        table_rows = conn.execute(
            "SELECT name FROM main.sqlite_master "
            "WHERE type IN ('table','view') AND lower(name)=?",
            (folded_table,),
        ).fetchall()
        if len(table_rows) != 1:
            raise ValueError("行级授权包含当前数据库中不存在或不唯一的表")
        table = str(table_rows[0][0])
        quoted_table = '"' + table.replace('"', '""') + '"'
        physical = [
            str(row[1]) for row in conn.execute(f'PRAGMA main.table_info({quoted_table})')
        ]
        by_folded = {column.casefold(): column for column in physical}
        visible_scope = allowed_columns.get(folded_table)
        visible = (
            [column for column in physical if column.casefold() in visible_scope]
            if visible_scope is not None else physical
        )
        if not visible:
            raise ValueError("行级授权表没有可见字段")
        predicates = []
        internal = {column.casefold() for column in visible}
        operator_sql = {
            "eq": "=", "neq": "!=", "gt": ">", "gte": ">=",
            "lt": "<", "lte": "<=",
        }
        for item in filters:
            column = by_folded.get(str(item["column"]).casefold())
            if column is None:
                raise ValueError(f"行级授权包含表 {table} 中不存在的字段")
            internal.add(column.casefold())
            quoted_column = '"' + column.replace('"', '""') + '"'
            operator = str(item["operator"])
            if operator in operator_sql:
                predicates.append(
                    f"{quoted_column} {operator_sql[operator]} {_sqlite_literal(item.get('value'))}"
                )
            elif operator in {"is_null", "is_not_null"}:
                predicates.append(
                    f"{quoted_column} IS {'NOT ' if operator == 'is_not_null' else ''}NULL"
                )
            else:
                values = item.get("value")
                if not isinstance(values, list) or not values:
                    raise ValueError("IN/NOT IN 行级过滤必须提供非空值列表")
                literals = ", ".join(_sqlite_literal(value) for value in values)
                predicates.append(
                    f"{quoted_column} {'NOT ' if operator == 'not_in' else ''}IN ({literals})"
                )
        projection = ", ".join(
            '"' + column.replace('"', '""') + '"' for column in visible
        )
        _create_sqlite_temp_view(conn,
            f"CREATE TEMP VIEW {quoted_table} AS SELECT {projection} "
            f"FROM main.{quoted_table} WHERE " + " AND ".join(f"({item})" for item in predicates)
        )
        internal_columns[folded_table] = frozenset(internal)
    return internal_columns


def _install_sqlite_scope_authorizer(
    conn: sqlite3.Connection,
    *,
    allowed_tables: Optional[frozenset[str]],
    allowed_columns: Dict[str, frozenset[str]],
    row_internal_columns: Optional[Dict[str, frozenset[str]]] = None,
    allow_writes: bool,
    unavailable_error: str,
) -> None:
    """Install the physical SQLite table/column boundary on one connection.

    Empty READ column names are emitted by SQLite for COUNT(*), so they are
    allowed after the table check. DELETE has no per-column callback and is
    denied for a column-scoped table; INSERT target columns are additionally
    checked by WriteSecurity because SQLITE_INSERT only reports the table.
    """
    if allowed_tables is None and not allowed_columns:
        return
    if not hasattr(conn, "set_authorizer"):
        raise RuntimeError(unavailable_error)

    def _authorize(
        action: int,
        arg1: Optional[str],
        arg2: Optional[str],
        database: Optional[str],
        source: Optional[str],
    ) -> int:
        table = str(arg1 or "").casefold()
        column = str(arg2 or "").casefold()
        if action == sqlite3.SQLITE_PRAGMA:
            return sqlite3.SQLITE_DENY
        if action == sqlite3.SQLITE_READ:
            row_columns = (row_internal_columns or {}).get(table)
            if row_columns is not None and str(database or "").casefold() == "main":
                if str(source or "").casefold() != table:
                    return sqlite3.SQLITE_DENY
                if column and column not in row_columns:
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK
            if allowed_tables is not None and table not in allowed_tables:
                return sqlite3.SQLITE_DENY
            columns = allowed_columns.get(table)
            if columns is not None and column and column not in columns:
                return sqlite3.SQLITE_DENY
        elif allow_writes and action in {
            sqlite3.SQLITE_INSERT,
            sqlite3.SQLITE_UPDATE,
            sqlite3.SQLITE_DELETE,
        }:
            if allowed_tables is not None and table not in allowed_tables:
                return sqlite3.SQLITE_DENY
            columns = allowed_columns.get(table)
            if columns is not None:
                if action == sqlite3.SQLITE_DELETE:
                    return sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_UPDATE and column not in columns:
                    return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    conn.set_authorizer(_authorize)

class SQLSecurity:
    """SQL 安全校验与执行：只读、单语句、禁写、LIMIT 强制、超时。"""

    # 禁止出现在 SQL 中的写操作/危险关键字
    FORBIDDEN = re.compile(
        r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|ATTACH|DETACH|VACUUM|"
        r"REINDEX|TRIGGER|PRAGMA|BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE|"
        r"GRANT|REVOKE)\b",
        re.IGNORECASE,
    )
    FORBIDDEN_FUNCTIONS = re.compile(
        r"\b(load_extension|readfile|writefile|fts3_tokenizer|"
        r"pg_read_file|pg_read_binary_file|pg_ls_dir|pg_stat_file|"
        r"lo_import|lo_export|dblink_connect|dblink_exec|"
        r"sys_eval|sys_exec|xp_cmdshell)\s*\(",
        re.IGNORECASE,
    )

    def __init__(
        self,
        connector: DBConnector,
        max_rows: int = 500,
        timeout_s: float = 15.0,
        allowed_tables: Optional[List[str]] = None,
        allowed_columns: Optional[Dict[str, List[str]]] = None,
        row_filters: Optional[Dict[str, List[dict]]] = None,
    ):
        self.connector = connector
        self.max_rows = max_rows
        self.timeout_s = timeout_s
        self.allowed_tables = (
            frozenset(str(name).casefold() for name in allowed_tables)
            if allowed_tables is not None else None
        )
        self.allowed_columns = _normalize_column_scope(allowed_columns)
        if self.allowed_columns and self.allowed_tables is None:
            raise ValueError("字段级授权必须建立在显式表级授权之上")
        self.row_filters = _normalize_row_scope(row_filters)
        if self.row_filters and self.allowed_tables is None:
            raise ValueError("行级授权必须建立在显式表级授权之上")
        self.row_filters = _normalize_row_scope(row_filters)
        if self.row_filters and self.allowed_tables is None:
            raise ValueError("行级授权必须建立在显式表级授权之上")

    def validate(self, sql: str) -> str:
        """校验 SQL：单语句、禁写操作；自动补 LIMIT；返回可执行 SQL。

        规则：
          - 只允许以 SELECT / WITH 开头（WITH 用于 CTE 查询）
          - 剥离单个结尾分号后不允许再出现分号（禁多语句）
          - FORBIDDEN 关键字命中即拒绝（写操作/危险操作）
          - 无 LIMIT 时自动追加 LIMIT max_rows（防大结果）
        """
        s, code = _normalize_single_sql_statement(
            sql, SQLSecurityError, "禁止多语句：只允许单条查询",
        )
        visible = code.strip()
        if not re.match(r"^(SELECT|WITH)\b", visible, re.IGNORECASE):
            raise SQLSecurityError("只允许 SELECT 查询（含 WITH CTE）")
        if self.FORBIDDEN.search(visible):
            raise SQLSecurityError("禁止写操作/危险关键字（INSERT/UPDATE/DELETE/DROP/ALTER/PRAGMA 等）")
        dangerous_function = self.FORBIDDEN_FUNCTIONS.search(visible)
        if not dangerous_function:
            function_tokens = _sql_code_only(s, mask_identifiers=False)
            function_tokens = function_tokens.translate(str.maketrans("", "", chr(34) + chr(96) + "[]"))
            dangerous_function = self.FORBIDDEN_FUNCTIONS.search(function_tokens)
        if dangerous_function:
            raise SQLSecurityError(f"禁止文件、扩展或系统函数: {dangerous_function.group(1)}")
        # 自动补 LIMIT（顶层无 LIMIT 时追加；聚合查询补上也无害）
        if not re.search(r"\bLIMIT\b", visible, re.IGNORECASE):
            # 另起一行可以同时避开末尾字符串与单行注释；SQLite/远程
            # 方言均把换行视为空白，真实 LIMIT 仍处于同一条语句中。
            s = f"{s}\nLIMIT {int(self.max_rows)}"
        return s

    def execute(self, sql: str) -> SQLResult:
        """在只读连接中执行（先校验），带超时，返回表格化结果。"""
        try:
            sql = self.validate(sql)
        except SQLSecurityError as e:
            return SQLResult(sql=sql, error=str(e))
        result = SQLResult(sql=sql)
        conn = self.connector.connect()
        connection_interrupted = False
        try:
            done = threading.Event()
            out: Dict[str, Any] = {}
            deadline = time.monotonic() + self.timeout_s

            def _check_progress() -> int:
                # 返回非 0 会中断正在执行的查询（sqlite3.OperationalError: interrupted）
                return 1 if time.monotonic() > deadline else 0

            def _worker() -> None:
                try:
                    if hasattr(conn, "set_progress_handler"):
                        conn.set_progress_handler(_check_progress, 500)
                    row_internal = _prepare_sqlite_row_views(
                        conn, self.row_filters, self.allowed_columns,
                    ) if self.row_filters else {}
                    _install_sqlite_scope_authorizer(
                        conn,
                        allowed_tables=self.allowed_tables,
                        allowed_columns=self.allowed_columns,
                        row_internal_columns=row_internal,
                        allow_writes=False,
                        unavailable_error="当前连接无法安全执行表/字段级授权查询",
                    )
                    cur = conn.cursor()
                    cur.execute(sql)
                    desc = cur.description
                    # Never materialize an arbitrarily large result set.  A
                    # model-supplied LIMIT may itself be huge, so fetch only
                    # enough rows to determine whether truncation occurred.
                    rows = cur.fetchmany(self.max_rows + 1)
                    out["desc"] = desc
                    out["rows"] = rows
                except Exception as e:  # noqa: BLE001 —— 任何错误都带回
                    out["error"] = e
                finally:
                    done.set()

            t = threading.Thread(target=_worker, daemon=True)
            t.start()
            if not done.wait(self.timeout_s + 0.5):
                # 兜底：progress handler 应已中断；仍没结束则关闭连接
                interrupt = getattr(self.connector, "interrupt", None)
                if callable(interrupt):
                    interrupt(conn)
                    connection_interrupted = True
                else:
                    self.connector.close(conn)
                done.wait(0.5)
                result.error = f"查询超时（>{self.timeout_s}s），已中断"
                return result
            # done 在工作线程 finally 中设置。Windows 上若不等待线程真正退出，
            # 紧接着清理临时数据库时可能短暂保留文件句柄，导致 WinError 32。
            t.join(timeout=0.5)
            if "error" in out:
                msg = str(out["error"])
                if "interrupted" in msg:
                    result.error = f"查询超时（>{self.timeout_s}s），已中断"
                else:
                    result.error = msg
                return result
            desc = out.get("desc") or []
            rows = out.get("rows") or []
            result.columns = [d[0] for d in desc]
            if len(rows) > self.max_rows:
                result.truncated = True
                rows = rows[: self.max_rows]
            # 值安全化（bytes/None 转 JSON 友好）
            result.rows = [self._safe_row(r) for r in rows]
            result.row_count = len(result.rows)
            return result
        finally:
            # A Windows DB-API connection can block in ``close()`` while a
            # worker is still inside a socket read.  ``interrupt`` already
            # invalidated that transport; let the daemon worker release its
            # final Python reference instead of turning a 250 ms timeout into
            # the full server-side query duration.
            if not connection_interrupted:
                self.connector.close(conn)

    @staticmethod
    def _safe_row(row: tuple) -> list:
        out = []
        for v in row:
            if isinstance(v, bytes):
                out.append(f"<blob {len(v)}B>")
            elif isinstance(v, Decimal):
                # Remote DB-API drivers expose DECIMAL/NUMERIC as Decimal.
                # Preserve a JSON numeric value so charts and numeric sorting
                # do not silently treat database measures as text.
                out.append(float(v))
            elif isinstance(v, (str, int, float, bool)) or v is None:
                out.append(v)
            elif isinstance(v, (date, datetime)):
                out.append(v.isoformat())
            else:
                out.append(str(v))
        return out


class SQLiteRelationalPlanRenderer:
    """Render a validated relational plan without accepting free-form SQL."""

    def __init__(self, schema: SchemaSnapshot):
        self.schema = schema

    @staticmethod
    def quote_identifier(value: str) -> str:
        return '"' + str(value).replace('"', '""') + '"'

    def _validate_column(self, ref: RelationalColumnRef) -> None:
        table = self.schema.tables.get(ref.table)
        if table is None or not any(column.name == ref.column for column in table.columns):
            raise ValueError(f"关系计划引用了不存在的列: {ref.table}.{ref.column}")

    def _validate_fk_edge(self, edge: RelationalJoinEdge) -> None:
        self._validate_column(edge.left)
        self._validate_column(edge.right)
        if edge.source != "foreign_key":
            raise ValueError("关系计划只允许 schema 声明的外键边")
        if edge.join_type not in {"INNER", "LEFT"}:
            raise ValueError("关系计划连接类型无效")
        left_column = next(
            column for column in self.schema.tables[edge.left.table].columns
            if column.name == edge.left.column
        )
        right_column = next(
            column for column in self.schema.tables[edge.right.table].columns
            if column.name == edge.right.column
        )
        left_declares_right = (
            left_column.fk_table == edge.right.table
            and left_column.fk_column == edge.right.column
        )
        right_declares_left = (
            right_column.fk_table == edge.left.table
            and right_column.fk_column == edge.left.column
        )
        if not (left_declares_right or right_declares_left):
            raise ValueError("关系计划连接边不是 schema 声明的真实外键")

    def _validate_filter(self, item: RelationalFilterPredicate, sources: set[str]) -> None:
        self._validate_column(item.column)
        if item.column.table not in sources:
            raise ValueError("过滤列不属于已声明数据源")
        if item.operator not in {"=", "!=", ">", ">=", "<", "<="}:
            raise ValueError("关系计划过滤操作符无效")
        if item.value_type == "number":
            if isinstance(item.value, bool) or not isinstance(item.value, (int, float)):
                raise ValueError("数值过滤的值类型无效")
        elif item.value_type == "text":
            if not isinstance(item.value, str) or len(item.value) > 512:
                raise ValueError("文本过滤的值类型无效")
        else:
            raise ValueError("关系计划过滤值类型无效")

    @staticmethod
    def _literal_sql(item: RelationalFilterPredicate) -> str:
        if item.value_type == "number":
            return str(item.value)
        return "'" + str(item.value).replace("'", "''") + "'"

    def _validated_aliases(self, plan: RelationalQueryPlan) -> Dict[str, str]:
        if plan.dialect != "sqlite":
            raise ValueError(f"SQLite 渲染器不支持方言: {plan.dialect}")
        if not plan.sources or len(plan.sources) != len(set(plan.sources)):
            raise ValueError("关系计划的数据源为空或重复")
        for table_name in plan.sources:
            if table_name not in self.schema.tables:
                raise ValueError(f"关系计划引用了不存在的表: {table_name}")
        for ref in [*plan.projections, *plan.group_keys]:
            self._validate_column(ref)
            if ref.table not in plan.sources:
                raise ValueError("关系计划列不属于已声明数据源")
        if not plan.projections or not plan.group_keys:
            raise ValueError("关系计划缺少投影列或分组键")
        if plan.aggregate.function != "COUNT" or plan.aggregate.distinct:
            raise ValueError("当前排名关系计划仅支持非去重 COUNT")
        if plan.aggregate.source_table not in plan.sources:
            raise ValueError("聚合事实表不属于已声明数据源")
        if plan.aggregate.column != "*":
            self._validate_column(RelationalColumnRef(
                plan.aggregate.source_table, plan.aggregate.column,
            ))
        if plan.ranking.direction not in {"ASC", "DESC"}:
            raise ValueError("关系计划排名方向无效")
        if plan.ranking.tie_policy not in {"single_row", "all_ties"}:
            raise ValueError("关系计划并列策略不完整")
        if plan.ranking.tie_policy == "single_row" and plan.ranking.limit != 1:
            raise ValueError("单行极值计划必须显式限制为 1 行")
        for edge in plan.joins:
            self._validate_fk_edge(edge)
            if edge.left.table not in plan.sources or edge.right.table not in plan.sources:
                raise ValueError("关系计划连接边超出已声明数据源")
        for item in plan.filters:
            self._validate_filter(item, set(plan.sources))
        if any(edge.join_type == "LEFT" for edge in plan.joins) and any(
            item.column.table != plan.sources[0] for item in plan.filters
        ):
            raise ValueError("保留零事实的 LEFT JOIN 计划不允许 WHERE 过滤右侧事实表")
        return {name: f"t{index}" for index, name in enumerate(plan.sources)}

    def _from_clause(self, plan: RelationalQueryPlan, aliases: Dict[str, str]) -> str:
        q = self.quote_identifier
        anchor = plan.sources[0]
        clauses = [f"FROM {q(anchor)} AS {aliases[anchor]}"]
        joined = {anchor}
        unused = list(plan.joins)
        for expected_table in plan.sources[1:]:
            candidates = [
                edge for edge in unused
                if (
                    edge.left.table == expected_table and edge.right.table in joined
                ) or (
                    edge.right.table == expected_table and edge.left.table in joined
                )
            ]
            if len(candidates) != 1:
                raise ValueError(
                    f"关系计划无法用唯一已声明边连接数据源: {expected_table}"
                )
            edge = candidates[0]
            unused.remove(edge)
            left = edge.left
            right = edge.right
            join_keyword = "LEFT JOIN" if edge.join_type == "LEFT" else "JOIN"
            clauses.append(
                f"{join_keyword} {q(expected_table)} AS {aliases[expected_table]} ON "
                f"{aliases[left.table]}.{q(left.column)} = "
                f"{aliases[right.table]}.{q(right.column)}"
            )
            joined.add(expected_table)
        if unused:
            raise ValueError("关系计划包含未消费或冗余的连接边")
        return "\n".join(clauses)

    def _filter_clause(
        self, filters: List[RelationalFilterPredicate], aliases: Dict[str, str],
    ) -> str:
        if not filters:
            return ""
        q = self.quote_identifier
        predicates = [
            f"{aliases[item.column.table]}.{q(item.column.column)} "
            f"{item.operator} {self._literal_sql(item)}"
            for item in filters
        ]
        return "\nWHERE " + " AND ".join(predicates)

    def _render_ranked(self, plan: RelationalQueryPlan) -> str:
        aliases = self._validated_aliases(plan)
        from_clause = self._from_clause(plan, aliases)
        q = self.quote_identifier

        def column_sql(ref: RelationalColumnRef) -> str:
            return f"{aliases[ref.table]}.{q(ref.column)}"

        projections = ", ".join(column_sql(ref) for ref in plan.projections)
        group_keys = ", ".join(column_sql(ref) for ref in plan.group_keys)
        count_target = (
            "*" if plan.aggregate.column == "*" else
            f"{aliases[plan.aggregate.source_table]}.{q(plan.aggregate.column)}"
        )
        aggregate_sql = f"COUNT({count_target})"
        filter_clause = self._filter_clause(plan.filters, aliases)
        direction = plan.ranking.direction
        if plan.ranking.tie_policy == "single_row":
            stable_order = ", ".join(
                f"{column_sql(ref)} ASC" for ref in plan.group_keys
            )
            return (
                f"SELECT {projections}\n{from_clause}{filter_clause}\n"
                f"GROUP BY {group_keys}\n"
                f"ORDER BY {aggregate_sql} {direction}, {stable_order}\nLIMIT 1"
            )

        inner_projection = ", ".join(
            f"{column_sql(ref)} AS {q(f'__dbagent_out_{index}')}"
            for index, ref in enumerate(plan.projections)
        )
        inner_keys = ", ".join(
            f"{column_sql(ref)} AS {q(f'__dbagent_key_{index}')}"
            for index, ref in enumerate(plan.group_keys)
        )
        final_projection = ", ".join(
            f"{q(f'__dbagent_out_{index}')} AS {q(ref.column)}"
            for index, ref in enumerate(plan.projections)
        )
        final_order = ", ".join(
            f"{q(f'__dbagent_key_{index}')} ASC"
            for index, _ref in enumerate(plan.group_keys)
        )
        extremum = "MAX" if direction == "DESC" else "MIN"
        return (
            f"WITH {q('__dbagent_grouped')} AS (\n"
            f"  SELECT {inner_projection}, {inner_keys}, "
            f"{aggregate_sql} AS {q(plan.aggregate.alias)}\n"
            f"  {from_clause.replace(chr(10), chr(10) + '  ')}"
            f"{filter_clause.replace(chr(10), chr(10) + '  ')}\n"
            f"  GROUP BY {group_keys}\n"
            f")\nSELECT {final_projection}\nFROM {q('__dbagent_grouped')}\n"
            f"WHERE {q(plan.aggregate.alias)} = "
            f"(SELECT {extremum}({q(plan.aggregate.alias)}) "
            f"FROM {q('__dbagent_grouped')})\nORDER BY {final_order}"
        )

    def _render_grouped_aggregate(
        self, plan: RelationalGroupedAggregatePlan,
    ) -> str:
        if plan.dialect != "sqlite" or not plan.sources \
                or len(plan.sources) != len(set(plan.sources)):
            raise ValueError("分组聚合计划的数据源无效")
        for table_name in plan.sources:
            if table_name not in self.schema.tables:
                raise ValueError(f"分组聚合计划引用了不存在的表: {table_name}")
        if not plan.projections or not plan.group_keys:
            raise ValueError("分组聚合计划缺少实体投影或稳定分组键")
        for ref in [*plan.projections, *plan.group_keys]:
            self._validate_column(ref)
            if ref.table not in plan.sources:
                raise ValueError("分组聚合列不属于已声明数据源")
        if plan.aggregate.function != "COUNT" or plan.aggregate.distinct \
                or plan.aggregate.source_table not in plan.sources:
            raise ValueError("当前分组聚合计划只支持一个非去重 COUNT")
        if not re.fullmatch(r"[A-Za-z_][\w$]*", plan.aggregate.alias):
            raise ValueError("分组聚合输出别名无效")
        if plan.aggregate.column == "*":
            if plan.include_zero:
                raise ValueError("保留零事实的分组计数不能使用 COUNT(*)")
        else:
            self._validate_column(RelationalColumnRef(
                plan.aggregate.source_table, plan.aggregate.column,
            ))
        for edge in plan.joins:
            self._validate_fk_edge(edge)
            if edge.left.table not in plan.sources or edge.right.table not in plan.sources:
                raise ValueError("分组聚合连接边超出已声明数据源")
            expected_type = "LEFT" if plan.include_zero else "INNER"
            if edge.join_type != expected_type:
                raise ValueError("分组聚合的连接类型与零事实语义不一致")
        for item in plan.filters:
            self._validate_filter(item, set(plan.sources))
        if plan.include_zero and any(
            item.column.table != plan.sources[0] for item in plan.filters
        ):
            raise ValueError("保留零事实的计划不允许 WHERE 过滤右侧事实表")

        aliases = {name: f"t{index}" for index, name in enumerate(plan.sources)}
        from_clause = self._from_clause(plan, aliases)
        filter_clause = self._filter_clause(plan.filters, aliases)
        q = self.quote_identifier

        def column_sql(ref: RelationalColumnRef) -> str:
            return f"{aliases[ref.table]}.{q(ref.column)}"

        projections = [column_sql(ref) for ref in plan.projections]
        count_target = (
            "*" if plan.aggregate.column == "*" else
            f"{aliases[plan.aggregate.source_table]}.{q(plan.aggregate.column)}"
        )
        projections.append(
            f"COUNT({count_target}) AS {q(plan.aggregate.alias)}"
        )
        group_keys = ", ".join(column_sql(ref) for ref in plan.group_keys)
        return (
            f"SELECT {', '.join(projections)}\n{from_clause}{filter_clause}\n"
            f"GROUP BY {group_keys}\nORDER BY {group_keys} ASC"
        )

    def _render_set(self, plan: RelationalSetQueryPlan) -> str:
        if plan.dialect != "sqlite" or plan.operator not in {"INTERSECT", "EXCEPT"}:
            raise ValueError("SQLite 集合计划只支持 INTERSECT/EXCEPT")
        if len(plan.branches) < 2 or len(plan.branches) > 6 \
                or (plan.operator == "EXCEPT" and len(plan.branches) != 2):
            raise ValueError("集合计划分支数无效")
        if plan.operator == "INTERSECT" and plan.proof_edges:
            raise ValueError("独立集合交集不应携带关联边")
        if plan.operator == "EXCEPT":
            if len(plan.proof_edges) != 1:
                raise ValueError("反集合计划缺少唯一外键证明")
            proof = plan.proof_edges[0]
            self._validate_fk_edge(proof)
            branch_refs = {
                (branch.projection.table, branch.projection.column)
                for branch in plan.branches
            }
            if {
                (proof.left.table, proof.left.column),
                (proof.right.table, proof.right.column),
            } != branch_refs:
                raise ValueError("反集合证明边与分支投影不一致")
        q = self.quote_identifier
        statements: List[str] = []
        for branch in plan.branches:
            if branch.source not in self.schema.tables \
                    or branch.projection.table != branch.source:
                raise ValueError("集合分支数据源或投影无效")
            self._validate_column(branch.projection)
            aliases = {branch.source: q(branch.source)}
            for item in branch.filters:
                self._validate_filter(item, {branch.source})
            where = self._filter_clause(branch.filters, aliases)
            statements.append(
                f"SELECT {q(branch.projection.column)} "
                f"FROM {q(branch.source)}{where}"
            )
        return f"\n{plan.operator}\n".join(statements)

    def _render_scalar(self, plan: RelationalScalarAggregatePlan) -> str:
        if plan.dialect != "sqlite" or plan.source not in self.schema.tables:
            raise ValueError("标量聚合数据源无效")
        if plan.aggregate.source_table != plan.source \
                or plan.aggregate.function != "COUNT" \
                or plan.aggregate.column == "*" \
                or not plan.aggregate.distinct:
            raise ValueError("当前标量聚合仅支持 COUNT(DISTINCT column)")
        ref = RelationalColumnRef(plan.source, plan.aggregate.column)
        self._validate_column(ref)
        for edge in plan.proof_edges:
            self._validate_fk_edge(edge)
        q = self.quote_identifier
        aliases = {plan.source: q(plan.source)}
        for item in plan.filters:
            self._validate_filter(item, {plan.source})
        where = self._filter_clause(plan.filters, aliases)
        return (
            f"SELECT COUNT(DISTINCT {q(plan.aggregate.column)}) "
            f"FROM {q(plan.source)}{where}"
        )

    def _render_scalar_ranking(self, plan: RelationalScalarRankingPlan) -> str:
        if plan.dialect != "sqlite" or not plan.sources \
                or len(plan.sources) != len(set(plan.sources)):
            raise ValueError("标量排名计划的数据源无效")
        if plan.direction not in {"ASC", "DESC"} or plan.limit != 1:
            raise ValueError("标量排名计划必须声明 ASC/DESC 和单行限制")
        for table_name in plan.sources:
            if table_name not in self.schema.tables:
                raise ValueError(f"标量排名计划引用了不存在的表: {table_name}")
        refs = [*plan.projections, plan.order_by, *plan.tie_breakers]
        if not plan.projections:
            raise ValueError("标量排名计划缺少投影")
        for ref in refs:
            self._validate_column(ref)
            if ref.table not in plan.sources:
                raise ValueError("标量排名列不属于已声明数据源")
        for edge in plan.joins:
            self._validate_fk_edge(edge)
            if edge.join_type != "INNER" \
                    or edge.left.table not in plan.sources \
                    or edge.right.table not in plan.sources:
                raise ValueError("标量排名只允许已声明数据源之间的内连接")
        for item in plan.filters:
            self._validate_filter(item, set(plan.sources))
        aliases = {name: f"t{index}" for index, name in enumerate(plan.sources)}
        from_clause = self._from_clause(plan, aliases)
        filter_clause = self._filter_clause(plan.filters, aliases)
        q = self.quote_identifier

        def column_sql(ref: RelationalColumnRef) -> str:
            return f"{aliases[ref.table]}.{q(ref.column)}"

        projections = ", ".join(column_sql(ref) for ref in plan.projections)
        order_items = [f"{column_sql(plan.order_by)} {plan.direction}"]
        order_items.extend(
            f"{column_sql(ref)} ASC" for ref in plan.tie_breakers
            if ref != plan.order_by
        )
        return (
            f"SELECT {projections}\n{from_clause}{filter_clause}\n"
            f"ORDER BY {', '.join(order_items)}\nLIMIT 1"
        )

    def render(self, plan: Any) -> str:
        if isinstance(plan, RelationalGroupedAggregatePlan):
            return self._render_grouped_aggregate(plan)
        if isinstance(plan, RelationalSetQueryPlan):
            return self._render_set(plan)
        if isinstance(plan, RelationalScalarAggregatePlan):
            return self._render_scalar(plan)
        if isinstance(plan, RelationalScalarRankingPlan):
            return self._render_scalar_ranking(plan)
        if not isinstance(plan, RelationalQueryPlan):
            raise ValueError("未知的本地关系计划类型")
        return self._render_ranked(plan)


class RelationalGroupedMetricsRenderer:
    """Render the bounded grouped-metrics IR for SQLite/MySQL/PostgreSQL."""

    _NUMERIC_TYPE_RE = re.compile(
        r"(?:INT|REAL|FLOA|DOUB|DEC|NUM|MONEY|SERIAL)", re.IGNORECASE,
    )

    def __init__(self, schema: SchemaSnapshot, dialect: str):
        self.schema = schema
        self.dialect = str(dialect or "sqlite").lower()
        if self.dialect not in {"sqlite", "mysql", "postgresql"}:
            raise ValueError("分组指标计划不支持当前数据库方言")

    def quote_identifier(self, value: str) -> str:
        raw = str(value)
        if self.dialect == "mysql":
            return "`" + raw.replace("`", "``") + "`"
        return '"' + raw.replace('"', '""') + '"'

    def _column(self, ref: RelationalColumnRef) -> DBColumn:
        table = self.schema.tables.get(ref.table)
        if table is None:
            raise ValueError(f"分组指标计划引用了不存在的表: {ref.table}")
        column = next((item for item in table.columns if item.name == ref.column), None)
        if column is None:
            raise ValueError(f"分组指标计划引用了不存在的列: {ref.table}.{ref.column}")
        return column

    def _validate_edge(self, edge: RelationalJoinEdge) -> None:
        left = self._column(edge.left)
        right = self._column(edge.right)
        if edge.join_type != "INNER":
            raise ValueError("分组指标计划当前只允许内连接")
        if edge.source == "foreign_key":
            if not (
                left.fk_table == edge.right.table
                and left.fk_column == edge.right.column
            ) and not (
                right.fk_table == edge.left.table
                and right.fk_column == edge.left.column
            ):
                raise ValueError("分组指标计划的外键边与 schema 不一致")
        elif edge.source != "explicit":
            raise ValueError("分组指标计划只接受声明外键或用户显式等值关系")

    @staticmethod
    def _literal(item: RelationalFilterPredicate) -> str:
        if item.value_type == "number":
            if isinstance(item.value, bool) or not isinstance(item.value, (int, float)):
                raise ValueError("分组指标数值过滤类型无效")
            return str(item.value)
        if item.value_type != "text" or not isinstance(item.value, str) \
                or len(item.value) > 512:
            raise ValueError("分组指标文本过滤类型无效")
        return "'" + item.value.replace("'", "''") + "'"

    def render(self, plan: RelationalGroupedMetricsPlan) -> str:
        if plan.dialect != self.dialect or not plan.sources \
                or len(plan.sources) != len(set(plan.sources)) \
                or not 1 <= len(plan.dimensions) <= 3 \
                or not 2 <= len(plan.aggregates) <= 6:
            raise ValueError("分组指标计划形状无效")
        for table_name in plan.sources:
            if table_name not in self.schema.tables:
                raise ValueError(f"分组指标计划引用了不存在的表: {table_name}")
        for ref in [*plan.dimensions, *plan.group_keys]:
            self._column(ref)
            if ref.table not in plan.sources:
                raise ValueError("分组指标列不属于已声明数据源")
        if plan.dimensions != plan.group_keys:
            raise ValueError("分组指标的可见维度与分组键必须一致")
        aliases = {name: f"t{index}" for index, name in enumerate(plan.sources)}
        q = self.quote_identifier

        clauses = [f"FROM {q(plan.sources[0])} AS {aliases[plan.sources[0]]}"]
        joined = {plan.sources[0]}
        unused = list(plan.joins)
        for expected_table in plan.sources[1:]:
            candidates = [
                edge for edge in unused
                if (edge.left.table == expected_table and edge.right.table in joined)
                or (edge.right.table == expected_table and edge.left.table in joined)
            ]
            if len(candidates) != 1:
                raise ValueError("分组指标计划无法用唯一关系边连接全部数据源")
            edge = candidates[0]
            self._validate_edge(edge)
            unused.remove(edge)
            clauses.append(
                f"JOIN {q(expected_table)} AS {aliases[expected_table]} ON "
                f"{aliases[edge.left.table]}.{q(edge.left.column)} = "
                f"{aliases[edge.right.table]}.{q(edge.right.column)}"
            )
            joined.add(expected_table)
        if unused:
            raise ValueError("分组指标计划包含冗余关系边")

        def ref_sql(ref: RelationalColumnRef) -> str:
            return f"{aliases[ref.table]}.{q(ref.column)}"

        select_items = [ref_sql(ref) for ref in plan.dimensions]
        aggregate_aliases = set()
        for aggregate in plan.aggregates:
            function = str(aggregate.function or "").upper()
            if function not in {"COUNT", "SUM", "AVG", "MIN", "MAX"} \
                    or aggregate.source_table not in plan.sources \
                    or not re.fullmatch(r"[A-Za-z_][\w$]*", aggregate.alias) \
                    or aggregate.alias.casefold() in aggregate_aliases:
                raise ValueError("分组指标聚合定义无效")
            aggregate_aliases.add(aggregate.alias.casefold())
            if aggregate.column == "*":
                if function != "COUNT" or aggregate.distinct:
                    raise ValueError("只有非去重 COUNT 可以使用星号")
                target = "*"
            else:
                ref = RelationalColumnRef(aggregate.source_table, aggregate.column)
                column = self._column(ref)
                if function != "COUNT" and not self._NUMERIC_TYPE_RE.search(column.type or ""):
                    raise ValueError("数值聚合只能使用 schema 声明的数值列")
                target = ref_sql(ref)
                if aggregate.distinct:
                    target = "DISTINCT " + target
            select_items.append(f"{function}({target}) AS {q(aggregate.alias)}")

        predicates = []
        for item in plan.filters:
            self._column(item.column)
            if item.column.table not in plan.sources \
                    or item.operator not in {"=", "!=", "<>", ">", ">=", "<", "<="}:
                raise ValueError("分组指标过滤定义无效")
            predicates.append(
                f"{ref_sql(item.column)} {item.operator} {self._literal(item)}"
            )

        order_items = []
        for term in plan.order_by:
            direction = str(term.direction or "").upper()
            if direction not in {"ASC", "DESC"}:
                raise ValueError("分组指标排序方向无效")
            if term.column is not None:
                self._column(term.column)
                target = ref_sql(term.column)
            elif term.aggregate_alias.casefold() in aggregate_aliases:
                target = q(term.aggregate_alias)
            else:
                raise ValueError("分组指标排序目标无效")
            order_items.append(f"{target} {direction}")
        if not order_items:
            order_items = [f"{ref_sql(ref)} ASC" for ref in plan.group_keys]
        elif any(term.aggregate_alias for term in plan.order_by):
            order_items.extend(
                f"{ref_sql(ref)} ASC" for ref in plan.group_keys
                if all(term.column != ref for term in plan.order_by)
            )

        return (
            f"SELECT {', '.join(select_items)}\n"
            + "\n".join(clauses)
            + (("\nWHERE " + " AND ".join(predicates)) if predicates else "")
            + "\nGROUP BY " + ", ".join(ref_sql(ref) for ref in plan.group_keys)
            + "\nORDER BY " + ", ".join(order_items)
        )


class CalendarFilterCompiler:
    """把有限业务日历请求编译为 SQLite 日期/时间戳上的确定性谓词。"""

    _FISCAL_QUARTER_RE = re.compile(
        r"(?<!\d)(?P<year>\d{4})\s*(?:财年|会计年度)\s*"
        r"(?:(?:第\s*)?(?P<quarter_cn>[1-4一二三四])\s*(?:季度|财季)|"
        r"[Qq]\s*(?P<quarter_q>[1-4]))",
        re.IGNORECASE,
    )

    _FISCAL_YEAR_RE = re.compile(
        r"(?<!\d)(?P<year>\d{4})\s*(?:财年|会计年度)", re.IGNORECASE,
    )
    _DATE_RANGE_RE = re.compile(
        r"(?:从|自)?\s*(?P<start>\d{4}-\d{2}-\d{2})\s*(?:到|至|~|～|—)\s*"
        r"(?P<end>\d{4}-\d{2}-\d{2})",
        re.IGNORECASE,
    )
    _WORKDAY_RE = re.compile(r"(?:工作日|营业日|交易日)", re.IGNORECASE)

    def __init__(self, schema: SchemaSnapshot, connector: Any):
        self.schema = schema
        self.connector = connector

    @staticmethod
    def quote_identifier(value: str) -> str:
        return '"' + str(value).replace('"', '""') + '"'

    @staticmethod
    def quote_text(value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    def _calendar_column(self, table_name: str, column_name: str) -> Optional[DBColumn]:
        table = self.schema.tables.get(table_name)
        if table is None:
            return None
        column = next((item for item in table.columns if item.name == column_name), None)
        return column

    def _day_expression(self, match: dict, calendar: dict) -> Optional[str]:
        column = self._calendar_column(match["table"], match["column"])
        if column is None:
            return None
        declared_type = re.sub(r"\s+", " ", str(column.type or "").strip()).upper()
        basis = calendar.get("storage_basis") or "unspecified"
        source = calendar.get("storage_basis_source") or "legacy_default"
        q = self.quote_identifier
        qualified = f"{q(match['table'])}.{q(match['column'])}"
        if basis == "declared_date" and declared_type == "DATE":
            return f"date({qualified})"
        if declared_type not in {"DATETIME", "TIMESTAMP"} or source != "explicit":
            return None
        if basis == "local_datetime":
            # 协议要求值已是业务本地墙上时间且不带时区后缀；这里不执行 IANA/DST 换算。
            return f"date({qualified})"
        if basis == "utc_datetime":
            conversion = str(calendar.get("timezone_conversion") or "fixed_offset")
            if conversion == "iana_tzdata":
                try:
                    TimezoneRuntime.require_zone(
                        str(calendar.get("timezone") or ""),
                        tzdata_version=str(calendar.get("tzdata_version") or ""),
                        iana_version=str(calendar.get("iana_version") or ""),
                    )
                    version_token = TimezoneRuntime.version_token_for(
                        str(calendar.get("tzdata_version") or ""),
                        str(calendar.get("iana_version") or ""),
                    )
                except ValueError:
                    return None
                zone = self.quote_text(str(calendar.get("timezone") or ""))
                version = self.quote_text(version_token)
                return (
                    f"{TimezoneRuntime.SQL_FUNCTION}({qualified}, {zone}, {version})"
                )
            if conversion != "fixed_offset":
                return None
            offset = calendar.get("business_utc_offset_minutes")
            if isinstance(offset, bool) or not isinstance(offset, int) or not -840 <= offset <= 840:
                return None
            return f"date({qualified}, '{offset:+d} minutes')"
        return None

    @staticmethod
    def _fiscal_start_year(label_year: int, calendar: dict) -> int:
        if calendar.get("fiscal_year_label") != "end_year":
            return label_year
        if (
            int(calendar["fiscal_year_start_month"]) == 1
            and int(calendar["fiscal_year_start_day"]) == 1
        ):
            return label_year
        return label_year - 1

    @staticmethod
    def _add_months_exact(value: date, months: int) -> Optional[date]:
        month_index = value.year * 12 + value.month - 1 + months
        year, month_zero = divmod(month_index, 12)
        month = month_zero + 1
        try:
            return date(year, month, value.day)
        except ValueError:
            # 非整月口径（例如 1 月 31 日起算）需要独立业务规则，不能擅自截到月末。
            return None

    def _workday_predicate(self, day_expr: str, calendar: dict) -> str:
        sqlite_weekends = sorted(0 if value == 7 else value for value in calendar["weekend_days"])
        weekday = "1=1"
        if sqlite_weekends:
            values = ", ".join(str(value) for value in sqlite_weekends)
            weekday = f"CAST(strftime('%w', {day_expr}) AS INTEGER) NOT IN ({values})"

        holiday_table = calendar.get("holiday_table")
        if not holiday_table:
            return f"({weekday})"
        alias = "__dbagent_calendar_h"
        q = self.quote_identifier
        match = (
            f"date({q(alias)}.{q(calendar['holiday_date_column'])}) = {day_expr}"
        )
        source = f"{q(holiday_table)} AS {q(alias)}"
        override = calendar.get("working_override_column")
        if not override:
            return f"(({weekday}) AND NOT EXISTS (SELECT 1 FROM {source} WHERE {match}))"

        override_expr = f"COALESCE(CAST({q(alias)}.{q(override)} AS INTEGER), 0)"
        working_override = (
            f"EXISTS (SELECT 1 FROM {source} WHERE {match} AND {override_expr} <> 0)"
        )
        holiday_override = (
            f"NOT EXISTS (SELECT 1 FROM {source} WHERE {match} AND {override_expr} = 0)"
        )
        # 同一天出现冲突记录时，显式工作日覆盖优先；否则工作日必须同时
        # 满足固定周末规则且不存在非工作日例外。
        return f"({working_override} OR (({weekday}) AND {holiday_override}))"

    def compile(self, question: str, semantic: SemanticResolution) -> Optional[CalendarFilterPlan]:
        calendars = [item for item in semantic.matches if item.get("kind") == "business_calendar"]
        dialect = str(getattr(self.connector, "dialect", "sqlite") or "sqlite").lower()
        if len(calendars) != 1 or dialect != "sqlite":
            return None
        match = calendars[0]
        calendar = match["calendar"]
        day_expr = self._day_expression(match, calendar)
        if day_expr is None:
            return None
        fiscal_quarter = self._FISCAL_QUARTER_RE.search(question)
        fiscal = None if fiscal_quarter else self._FISCAL_YEAR_RE.search(question)
        explicit_range = self._DATE_RANGE_RE.search(question)
        workdays = bool(self._WORKDAY_RE.search(question))
        fiscal_period = fiscal_quarter or fiscal
        if explicit_range and fiscal_period:
            return None
        if workdays and not (explicit_range or fiscal_period):
            return None
        if not fiscal_period and not (workdays and explicit_range):
            return None

        end_inclusive = False
        fiscal_quarter_number: Optional[int] = None
        if fiscal_period:
            if calendar.get("fiscal_year_label_source") != "explicit":
                return None
            label_year = int(fiscal_period.group("year"))
            start_year = self._fiscal_start_year(label_year, calendar)
            try:
                fiscal_start = date(
                    start_year,
                    int(calendar["fiscal_year_start_month"]),
                    int(calendar["fiscal_year_start_day"]),
                )
            except ValueError:
                return None
            if fiscal_quarter:
                quarter_token = (
                    fiscal_quarter.group("quarter_cn") or fiscal_quarter.group("quarter_q")
                )
                fiscal_quarter_number = (
                    int(quarter_token) if quarter_token.isdigit()
                    else {"一": 1, "二": 2, "三": 3, "四": 4}.get(quarter_token, 0)
                )
                if fiscal_quarter_number not in {1, 2, 3, 4}:
                    return None
                start = self._add_months_exact(fiscal_start, (fiscal_quarter_number - 1) * 3)
                end = self._add_months_exact(fiscal_start, fiscal_quarter_number * 3)
                if start is None or end is None:
                    return None
            else:
                start = fiscal_start
                end = date(
                    start_year + 1,
                    int(calendar["fiscal_year_start_month"]),
                    int(calendar["fiscal_year_start_day"]),
                )
        else:
            try:
                start = date.fromisoformat(explicit_range.group("start"))
                end = date.fromisoformat(explicit_range.group("end"))
            except ValueError:
                return None
            if start > end:
                return None
            end_inclusive = True

        end_operator = "<=" if end_inclusive else "<"
        predicates = [
            f"{day_expr} >= '{start.isoformat()}'",
            f"{day_expr} {end_operator} '{end.isoformat()}'",
        ]
        if workdays:
            predicates.append(self._workday_predicate(day_expr, calendar))
        if fiscal_quarter:
            mode = "fiscal_quarter_business_days" if workdays else "fiscal_quarter"
        else:
            mode = "fiscal_business_days" if fiscal and workdays else (
                "fiscal_year" if fiscal else "business_days"
            )
        return CalendarFilterPlan(
            mode=mode,
            table=match["table"],
            column=match["column"],
            calendar_term=match["term"],
            date_range={
                "start": start.isoformat(),
                "end": end.isoformat(),
                "end_inclusive": end_inclusive,
            },
            predicate=" AND ".join(f"({item})" for item in predicates),
            rules={
                "fiscal_year_label": calendar["fiscal_year_label"],
                "fiscal_year_label_source": calendar["fiscal_year_label_source"],
                "fiscal_year_start": (
                    f"{calendar['fiscal_year_start_month']:02d}-"
                    f"{calendar['fiscal_year_start_day']:02d}"
                ),
                "timezone": calendar["timezone"],
                "storage_basis": calendar["storage_basis"],
                "storage_basis_source": calendar["storage_basis_source"],
                "business_utc_offset_minutes": calendar.get("business_utc_offset_minutes"),
                "timezone_conversion": calendar.get("timezone_conversion") or "none",
                "tzdata_version": calendar.get("tzdata_version"),
                "iana_version": calendar.get("iana_version"),
                "fiscal_quarter": fiscal_quarter_number,
                "week_start_iso": calendar["week_start"],
                "weekend_days_iso": list(calendar["weekend_days"]),
                "holiday_table": calendar.get("holiday_table") or None,
                "holiday_date_column": calendar.get("holiday_date_column") or None,
                "working_override_column": calendar.get("working_override_column") or None,
            },
        )


class DeterministicCalendarQueryExecutor:
    """执行严格形状的单表日历聚合；不支持时返回 None 交给现有 NL2SQL。"""

    _COUNT_MEASURE_RE = re.compile(
        r"(?:订单|记录|数据)?(?:数量|总数|数)|多少\s*(?:条|个)?(?:订单|记录|数据)?",
        re.IGNORECASE,
    )

    def __init__(self, security: SQLSecurity, schema: SchemaSnapshot, connector: Any):
        self.security = security
        self.schema = schema
        self.compiler = CalendarFilterCompiler(schema, connector)

    @staticmethod
    def _literal(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("非有限数值不能进入确定性 SQL")
            return repr(value)
        return "'" + str(value).replace("'", "''") + "'"

    def _filter_sql(self, table_name: str, item: dict) -> str:
        q = self.compiler.quote_identifier
        column = f"{q(table_name)}.{q(item['column'])}"
        operator = item["operator"]
        if operator == "is_null":
            return f"{column} IS NULL"
        if operator == "is_not_null":
            return f"{column} IS NOT NULL"
        if operator in {"in", "not_in"}:
            values = ", ".join(self._literal(value) for value in item["value"])
            keyword = "IN" if operator == "in" else "NOT IN"
            return f"{column} {keyword} ({values})"
        sql_operator = {
            "eq": "=", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
        }[operator]
        return f"{column} {sql_operator} {self._literal(item['value'])}"

    def _supported_shape(
        self,
        question: str,
        plan: CalendarFilterPlan,
        matches: List[dict],
        measure_term: str,
        is_count: bool,
    ) -> bool:
        remaining = str(question)
        remaining = CalendarFilterCompiler._FISCAL_QUARTER_RE.sub(" ", remaining)
        remaining = CalendarFilterCompiler._FISCAL_YEAR_RE.sub(" ", remaining)
        remaining = CalendarFilterCompiler._DATE_RANGE_RE.sub(" ", remaining)
        remaining = CalendarFilterCompiler._WORKDAY_RE.sub(" ", remaining)
        for item in matches:
            if item.get("kind") in {"table_alias", "time_field", "business_calendar", "enum_value"}:
                remaining = re.sub(re.escape(str(item.get("term") or "")), " ", remaining, flags=re.I)
        for token in (
            f"{plan.table}.{plan.column}", plan.table, plan.column,
        ):
            remaining = re.sub(re.escape(token), " ", remaining, flags=re.I)
        if is_count:
            remaining = self._COUNT_MEASURE_RE.sub(" ", remaining)
        else:
            remaining = re.sub(re.escape(measure_term), " ", remaining, flags=re.I)
        remaining = re.sub(r"(?:请帮我|帮我|麻烦|请|统计|计算|查询|查一下|一下|按|内|期间|之间|范围内|范围|"
                           r"时间字段|时间范围|时间粒度|业务日历|指标口径|聚合字段|目标表|目标字段|新增内容|关联条件)", " ", remaining)
        remaining = re.sub(r"[\s，,。！？!?；;：:·的]+", "", remaining)
        return not remaining

    def answer(self, question: str, semantic: SemanticResolution) -> Optional[DBAnswer]:
        plan = self.compiler.compile(question, semantic)
        if plan is None:
            return None
        matches = semantic.matches
        if any(item.get("kind") in {"ratio_metric", "dimension", "column_alias"} for item in matches):
            return None
        if any(
            item.get("kind") == "enum_value" and item.get("table") != plan.table
            for item in matches
        ):
            return None
        metrics = [
            item for item in matches
            if item.get("kind") == "metric" and item.get("table") == plan.table
        ]
        if len(metrics) > 1:
            return None

        q = self.compiler.quote_identifier
        filters: List[str] = [plan.predicate]
        for item in matches:
            if item.get("kind") == "enum_value" and item.get("table") == plan.table:
                filters.append(
                    f"{q(plan.table)}.{q(item['column'])} = {self._literal(item['value'])}"
                )
        is_count = not metrics
        if metrics:
            metric = metrics[0]
            aggregation = str(metric["aggregation"]).upper()
            column = metric.get("column")
            if aggregation == "COUNT":
                expression = f"COUNT({q(plan.table)}.{q(column)})" if column else "COUNT(*)"
            elif aggregation == "COUNT_DISTINCT":
                if not column:
                    return None
                expression = f"COUNT(DISTINCT {q(plan.table)}.{q(column)})"
            else:
                if not column or aggregation not in {"SUM", "AVG", "MIN", "MAX"}:
                    return None
                expression = f"{aggregation}({q(plan.table)}.{q(column)})"
            for item in metric.get("filters") or []:
                filters.append(self._filter_sql(plan.table, item))
            measure_term = metric["term"]
        else:
            if not self._COUNT_MEASURE_RE.search(question):
                return None
            expression = "COUNT(*)"
            measure_term = "记录数"

        if not self._supported_shape(question, plan, matches, measure_term, is_count):
            return None
        sql = (
            f"SELECT {expression} AS {q(measure_term)} FROM {q(plan.table)} "
            f"WHERE " + " AND ".join(f"({item})" for item in filters)
        )
        result = self.security.execute(sql)
        plan.sql = result.sql
        if result.error:
            plan.status = "failed"
            return DBAnswer(
                kind="error",
                narrative=f"确定性业务日历查询失败：{result.error}",
                sql=result.sql,
                error=result.error,
                calendar_plan=plan.as_dict(),
            )
        plan.status = "executed"
        end_symbol = "≤" if plan.date_range["end_inclusive"] else "<"
        narrative = (
            f"已按确定性业务日历执行：{plan.date_range['start']} ≤ {plan.table}.{plan.column} "
            f"{end_symbol} {plan.date_range['end']}。"
        )
        return DBAnswer(
            kind="query",
            narrative=narrative,
            sql=result.sql,
            columns=list(result.columns),
            rows=result.rows,
            calendar_plan=plan.as_dict(),
            steps=[{
                "tool": "calendar_filter",
                "mode": plan.mode,
                "dialect": plan.dialect,
                "status": plan.status,
            }],
        )


class DeterministicMultiMetricQueryExecutor:
    """执行同表 2–6 个受控普通指标；不完整形状整体回退现有链路。"""

    MIN_MEASURES = 2
    MAX_MEASURES = 6

    def __init__(self, security: SQLSecurity, schema: SchemaSnapshot, connector: Any):
        self.security = security
        self.schema = schema
        self.connector = connector

    @staticmethod
    def _quote(value: str) -> str:
        return CalendarFilterCompiler.quote_identifier(value)

    @staticmethod
    def _literal(value: Any) -> str:
        return DeterministicCalendarQueryExecutor._literal(value)

    def _filter_sql(self, table_name: str, item: dict) -> str:
        column = f"{self._quote(table_name)}.{self._quote(item['column'])}"
        operator = item["operator"]
        if operator == "is_null":
            return f"{column} IS NULL"
        if operator == "is_not_null":
            return f"{column} IS NOT NULL"
        if operator in {"in", "not_in"}:
            values = ", ".join(self._literal(value) for value in item["value"])
            keyword = "IN" if operator == "in" else "NOT IN"
            return f"{column} {keyword} ({values})"
        sql_operator = {
            "eq": "=", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
        }[operator]
        return f"{column} {sql_operator} {self._literal(item['value'])}"

    @staticmethod
    def _question_order(question: str, metrics: List[dict]) -> List[dict]:
        folded = question.casefold()
        return sorted(
            metrics,
            key=lambda item: folded.find(str(item.get("term") or "").casefold()),
        )

    def _measure_expression(self, table_name: str, metric: dict) -> Optional[str]:
        aggregation = str(metric.get("aggregation") or "").upper()
        column_name = str(metric.get("column") or "")
        column = (
            f"{self._quote(table_name)}.{self._quote(column_name)}"
            if column_name else ""
        )
        predicates = [
            self._filter_sql(table_name, item)
            for item in metric.get("filters") or []
        ]
        condition = " AND ".join(f"({item})" for item in predicates)
        if not condition:
            if aggregation == "COUNT":
                return f"COUNT({column})" if column else "COUNT(*)"
            if aggregation == "COUNT_DISTINCT" and column:
                return f"COUNT(DISTINCT {column})"
            if aggregation in {"SUM", "AVG", "MIN", "MAX"} and column:
                return f"{aggregation}({column})"
            return None
        if aggregation == "COUNT":
            value = column or "1"
            return f"COUNT(CASE WHEN {condition} THEN {value} END)"
        if aggregation == "COUNT_DISTINCT" and column:
            return f"COUNT(DISTINCT CASE WHEN {condition} THEN {column} END)"
        if aggregation in {"SUM", "AVG", "MIN", "MAX"} and column:
            return f"{aggregation}(CASE WHEN {condition} THEN {column} END)"
        return None

    @staticmethod
    def _supported_shape(question: str, matches: List[dict], table_name: str) -> bool:
        remaining = str(question)
        for item in matches:
            remaining = re.sub(
                re.escape(str(item.get("term") or "")), " ", remaining,
                flags=re.IGNORECASE,
            )
        remaining = re.sub(re.escape(table_name), " ", remaining, flags=re.IGNORECASE)
        remaining = re.sub(
            r"(?:请帮我|帮我|麻烦|请|给我|统计|计算|查询|查一下|一下|汇总|聚合|"
            r"查看|展示|显示|返回|列出|分别|各自|逐个|分开|对比|比较)",
            " ", remaining, flags=re.IGNORECASE,
        )
        remaining = re.sub(
            r"(?:以及|并且|而且|同时|还有|和|与|及)", " ", remaining,
            flags=re.IGNORECASE,
        )
        remaining = re.sub(r"[\s，,。！？!?；;：:、·的]+", "", remaining)
        return not remaining

    def answer(self, question: str, semantic: SemanticResolution) -> Optional[DBAnswer]:
        dialect = str(getattr(self.connector, "dialect", "sqlite") or "sqlite").lower()
        if dialect != "sqlite":
            return None
        metrics = [item for item in semantic.matches if item.get("kind") == "metric"]
        if not self.MIN_MEASURES <= len(metrics) <= self.MAX_MEASURES:
            return None
        table_names = {str(item.get("table") or "") for item in metrics}
        if len(table_names) != 1:
            return None
        table_name = next(iter(table_names))
        if not table_name or table_name not in self.schema.tables:
            return None
        if any(
            item.get("kind") not in {"table_alias", "enum_value", "metric"}
            or item.get("table") != table_name
            for item in semantic.matches
        ):
            return None
        enum_matches = [
            item for item in semantic.matches if item.get("kind") == "enum_value"
        ]
        if len(enum_matches) > 1 or not self._supported_shape(
            question, semantic.matches, table_name,
        ):
            return None

        ordered_metrics = self._question_order(question, metrics)
        select_items: List[str] = []
        measures: List[dict] = []
        for metric in ordered_metrics:
            expression = self._measure_expression(table_name, metric)
            if expression is None:
                return None
            measure = {
                "term": metric["term"],
                "aggregation": metric["aggregation"],
                "column": str(metric.get("column") or ""),
                "filters": [dict(item) for item in metric.get("filters") or []],
            }
            measures.append(measure)
            select_items.append(f"{expression} AS {self._quote(metric['term'])}")

        global_filters = [{
            "column": item["column"], "operator": "eq", "value": item["value"],
        } for item in enum_matches]
        predicates = [self._filter_sql(table_name, item) for item in global_filters]
        sql = (
            "SELECT " + ", ".join(select_items)
            + f" FROM {self._quote(table_name)}"
            + ((" WHERE " + " AND ".join(f"({item})" for item in predicates))
               if predicates else "")
        )
        plan = MultiMetricAggregatePlan(
            table=table_name,
            measures=measures,
            global_filters=global_filters,
        )
        result = self.security.execute(sql)
        plan.sql = result.sql
        if result.error:
            plan.status = "failed"
            return DBAnswer(
                kind="error",
                narrative=f"确定性并列指标查询失败：{result.error}",
                sql=result.sql,
                error=result.error,
                metric_plan=plan.as_dict(),
            )
        plan.status = "executed"
        terms = "、".join(item["term"] for item in measures)
        return DBAnswer(
            kind="query",
            narrative=f"已在同一数据快照中完成 {len(measures)} 个受控指标：{terms}。",
            sql=result.sql,
            columns=list(result.columns),
            rows=result.rows,
            metric_plan=plan.as_dict(),
            steps=[{
                "tool": "multi_metric_aggregate",
                "measures": len(measures),
                "dialect": plan.dialect,
                "status": plan.status,
            }],
        )


class DeterministicDimensionQueryExecutor:
    """执行严格形状的 SQLite 单表维度聚合；复杂请求完整回退 NL2SQL。"""

    _COUNT_MEASURE_RE = DeterministicCalendarQueryExecutor._COUNT_MEASURE_RE
    _GROUP_SIGNAL_RE = re.compile(
        r"(按|根据|分组|各(?:个|类)|分别|\bgroup\s+by\b|\bbreakdown\b)",
        re.IGNORECASE,
    )

    def __init__(
        self,
        security: SQLSecurity,
        schema: SchemaSnapshot,
        connector: Any,
        semantic_catalog: SemanticCatalog,
    ):
        self.security = security
        self.schema = schema
        self.connector = connector
        self.semantic_catalog = semantic_catalog

    @staticmethod
    def _quote(value: str) -> str:
        return CalendarFilterCompiler.quote_identifier(value)

    @staticmethod
    def _literal(value: Any) -> str:
        return DeterministicCalendarQueryExecutor._literal(value)

    def _filter_sql(self, table_name: str, item: dict) -> str:
        column = f"{self._quote(table_name)}.{self._quote(item['column'])}"
        operator = item["operator"]
        if operator == "is_null":
            return f"{column} IS NULL"
        if operator == "is_not_null":
            return f"{column} IS NOT NULL"
        if operator in {"in", "not_in"}:
            values = ", ".join(self._literal(value) for value in item["value"])
            keyword = "IN" if operator == "in" else "NOT IN"
            return f"{column} {keyword} ({values})"
        sql_operator = {
            "eq": "=", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
        }[operator]
        return f"{column} {sql_operator} {self._literal(item['value'])}"

    def _selected_dimensions(
        self,
        question: str,
        semantic: SemanticResolution,
    ) -> Optional[tuple[str, List[dict], Optional[dict]]]:
        drill = self.semantic_catalog.dimension_drill_request(question)
        if drill is not None:
            if drill.get("status") != "resolved" or not drill.get("dimensions"):
                return None
            source = drill["source"]
            target = drill["target"]
            hierarchy = {
                "name": source["hierarchy"]["name"],
                "from_term": source["term"],
                "from_level": source["hierarchy"]["level"],
                "to_term": target["term"],
                "to_level": target["hierarchy"]["level"],
            }
            return "drilldown", list(drill["dimensions"]), hierarchy
        dimensions = [item for item in semantic.matches if item.get("kind") == "dimension"]
        if len(dimensions) != 1 or not self._GROUP_SIGNAL_RE.search(question):
            return None
        item = dimensions[0]
        dimension = {
            "term": item["term"],
            "table": item["table"],
            "column": item["column"],
            "level": item.get("hierarchy", {}).get("level"),
            "filters": [dict(filter_item) for filter_item in item.get("filters") or []],
        }
        hierarchy = None
        if item.get("hierarchy"):
            hierarchy = {
                "name": item["hierarchy"]["name"],
                "from_term": item["term"],
                "from_level": item["hierarchy"]["level"],
                "to_term": item["term"],
                "to_level": item["hierarchy"]["level"],
            }
        return "group_by", [dimension], hierarchy

    def _measures(
        self,
        question: str,
        matches: List[dict],
        table_name: str,
    ) -> Optional[tuple[List[str], List[dict], List[dict], bool]]:
        metrics = [
            item for item in matches
            if item.get("kind") == "metric" and item.get("table") == table_name
        ]
        if len(metrics) > DeterministicMultiMetricQueryExecutor.MAX_MEASURES:
            return None
        if not metrics:
            if not self._COUNT_MEASURE_RE.search(question):
                return None
            return ["COUNT(*)"], [{
                "term": "记录数", "aggregation": "count", "column": "", "filters": [],
            }], [], True
        metrics = DeterministicMultiMetricQueryExecutor._question_order(question, metrics)
        isolate_filters = len(metrics) > 1
        expressions: List[str] = []
        measures: List[dict] = []
        sql_filters: List[dict] = []
        for metric in metrics:
            expression_metric = metric
            if not isolate_filters:
                expression_metric = {**metric, "filters": []}
            expression = DeterministicMultiMetricQueryExecutor._measure_expression(
                self, table_name, expression_metric,
            )
            if expression is None:
                return None
            filters = [dict(item) for item in metric.get("filters") or []]
            expressions.append(expression)
            measures.append({
                "term": metric["term"],
                "aggregation": metric["aggregation"],
                "column": str(metric.get("column") or ""),
                "filters": filters,
            })
            if not isolate_filters:
                sql_filters.extend(filters)
        return expressions, measures, sql_filters, False

    def _supported_shape(
        self,
        question: str,
        matches: List[dict],
        dimensions: List[dict],
        is_count: bool,
    ) -> bool:
        remaining = str(question)
        for item in matches:
            remaining = re.sub(
                re.escape(str(item.get("term") or "")), " ", remaining, flags=re.IGNORECASE,
            )
        for item in dimensions:
            remaining = re.sub(
                re.escape(str(item.get("term") or "")), " ", remaining, flags=re.IGNORECASE,
            )
        if is_count:
            remaining = self._COUNT_MEASURE_RE.sub(" ", remaining)
        remaining = re.sub(
            r"(?:下钻|钻取|细分|展开)\s*(?:到|至)?\s*(?:(?:一|1)\s*(?:级|层)|下一(?:级|层))?",
            " ", remaining, flags=re.IGNORECASE,
        )
        remaining = re.sub(r"维度层级\s*[:：]", " ", remaining, flags=re.IGNORECASE)
        remaining = re.sub(
            r"(?:请帮我|帮我|麻烦|请|统计|计算|查询|查一下|一下|分组|分别|每个|各个|按|根据|从|到|至)",
            " ", remaining, flags=re.IGNORECASE,
        )
        remaining = re.sub(
            r"(?:以及|并且|而且|同时|还有|和|与|及)", " ", remaining,
            flags=re.IGNORECASE,
        )
        remaining = re.sub(r"[\s，,。！？!?；;：:、·的]+", "", remaining)
        return not remaining

    def answer(self, question: str, semantic: SemanticResolution) -> Optional[DBAnswer]:
        dialect = str(getattr(self.connector, "dialect", "sqlite") or "sqlite").lower()
        if dialect != "sqlite":
            return None
        selected = self._selected_dimensions(question, semantic)
        if selected is None:
            return None
        mode, dimensions, hierarchy = selected
        table_names = {str(item.get("table") or "") for item in dimensions}
        if len(table_names) != 1:
            return None
        table_name = next(iter(table_names))
        matches = semantic.matches
        if any(item.get("kind") in {"ratio_metric", "time_field", "business_calendar", "column_alias"} for item in matches):
            return None
        if any(
            item.get("kind") in {"dimension", "metric", "enum_value"}
            and item.get("table") != table_name
            for item in matches
        ):
            return None
        measure_result = self._measures(question, matches, table_name)
        if measure_result is None:
            return None
        expressions, measure_infos, sql_filters, is_count = measure_result
        dimension_filters: List[dict] = []
        seen_dimension_filters: set[str] = set()
        for dimension in dimensions:
            for item in dimension.get("filters") or []:
                key = json.dumps(item, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
                if key in seen_dimension_filters:
                    continue
                seen_dimension_filters.add(key)
                dimension_filters.append({
                    "dimension_term": dimension.get("term") or "",
                    **dict(item),
                })
        if len(dimension_filters) > SemanticCatalog.MAX_FILTERS:
            return None
        sql_filters.extend(dimension_filters)
        global_filters = [{
            "column": item["column"], "operator": "eq", "value": item["value"],
        } for item in matches if item.get("kind") == "enum_value"]
        if len(measure_infos) > 1 and len(global_filters) > 1:
            return None
        sql_filters.extend(global_filters)
        if not self._supported_shape(
            question, matches, dimensions, is_count,
        ):
            return None

        q = self._quote
        select_groups = [
            f"{q(table_name)}.{q(item['column'])} AS {q(item['term'])}"
            for item in dimensions
        ]
        group_columns = [f"{q(table_name)}.{q(item['column'])}" for item in dimensions]
        predicates = [self._filter_sql(table_name, item) for item in sql_filters]
        select_measures = [
            f"{expression} AS {q(measure_info['term'])}"
            for expression, measure_info in zip(expressions, measure_infos)
        ]
        sql = (
            "SELECT " + ", ".join([
                *select_groups,
                *select_measures,
            ])
            + f" FROM {q(table_name)}"
            + ((" WHERE " + " AND ".join(f"({item})" for item in predicates)) if predicates else "")
            + " GROUP BY " + ", ".join(group_columns)
            + " ORDER BY " + ", ".join(group_columns)
        )
        plan = DimensionAggregatePlan(
            mode=mode,
            table=table_name,
            dimensions=[dict(item) for item in dimensions],
            measure=measure_infos[0],
            measures=measure_infos,
            hierarchy=hierarchy,
            filters=[
                *[item for measure in measure_infos for item in measure.get("filters") or []],
                *dimension_filters,
                *global_filters,
            ],
            dimension_filters=dimension_filters,
            global_filters=global_filters,
        )
        result = self.security.execute(sql)
        plan.sql = result.sql
        if result.error:
            plan.status = "failed"
            return DBAnswer(
                kind="error",
                narrative=f"确定性业务维度查询失败：{result.error}",
                sql=result.sql,
                error=result.error,
                dimension_plan=plan.as_dict(),
            )
        plan.status = "executed"
        group_text = " → ".join(item["term"] for item in dimensions)
        narrative = (
            f"已按受控业务维度执行{'下钻' if mode == 'drilldown' else '分组'}："
            f"{group_text}，指标为{'、'.join(item['term'] for item in measure_infos)}。"
        )
        return DBAnswer(
            kind="query",
            narrative=narrative,
            sql=result.sql,
            columns=list(result.columns),
            rows=result.rows,
            dimension_plan=plan.as_dict(),
            steps=[{
                "tool": "dimension_aggregate",
                "mode": mode,
                "measures": len(measure_infos),
                "dialect": plan.dialect,
                "status": plan.status,
            }],
        )


class DeterministicTrendQueryExecutor:
    """执行严格形状的 SQLite 单表时间趋势聚合；复杂请求完整回退 NL2SQL。"""

    _COUNT_MEASURE_RE = DeterministicCalendarQueryExecutor._COUNT_MEASURE_RE
    _GRAIN_PATTERNS = {
        "day": re.compile(r"(?:按|每)\s*(?:日|天)|日度|\bdaily\b", re.IGNORECASE),
        "week": re.compile(r"(?:按|每)\s*周|周度|\bweekly\b", re.IGNORECASE),
        "month": re.compile(r"(?:按|每)\s*月|月度|\bmonthly\b", re.IGNORECASE),
        "quarter": re.compile(
            r"(?:按|每)\s*(?:季度|季)|季度|\bquarterly\b", re.IGNORECASE,
        ),
        "year": re.compile(r"(?:按|每)\s*年|年度|\byearly\b", re.IGNORECASE),
    }
    _GRAIN_LABEL_RE = re.compile(
        r"时间粒度\s*[:：]\s*(?:日|天|周|月|季度|季|年)", re.IGNORECASE,
    )
    _TREND_SIGNAL_RE = re.compile(r"(?:趋势|走势|变化|时序|\btrend\b)", re.IGNORECASE)
    _RELATIVE_RANGE_RE = re.compile(
        r"(?:最近|近|过去)\s*(?P<count>[1-9]\d{0,3})\s*(?P<unit>天|日|周)",
        re.IGNORECASE,
    )
    _RELATIVE_SIGNAL_RE = re.compile(
        r"(?:最近|近|过去)\s*\d+\s*(?:天|日|周|个月|月|季度|年)",
        re.IGNORECASE,
    )
    _REFERENCE_DATE_RE = re.compile(
        r"(?:截至|截止(?:到)?|以)\s*(?P<date>\d{4}-\d{2}-\d{2})"
        r"(?:\s*(?:为基准|为止|止))?",
        re.IGNORECASE,
    )
    MAX_RELATIVE_DAYS = 3660
    _BUCKET_LABELS = {
        "day": "日期",
        "week": "周起始日",
        "month": "月份",
        "quarter": "季度",
        "year": "年份",
    }

    def __init__(
        self,
        security: SQLSecurity,
        schema: SchemaSnapshot,
        connector: Any,
        semantic_catalog: SemanticCatalog,
        reference_date: Optional[date] = None,
    ):
        self.security = security
        self.schema = schema
        self.connector = connector
        self.semantic_catalog = semantic_catalog
        self.calendar_compiler = CalendarFilterCompiler(schema, connector)
        if isinstance(reference_date, datetime):
            reference_date = reference_date.date()
        if reference_date is not None and not isinstance(reference_date, date):
            raise ValueError("趋势参考日必须是 date")
        self.reference_date = reference_date

    @staticmethod
    def _quote(value: str) -> str:
        return CalendarFilterCompiler.quote_identifier(value)

    @staticmethod
    def _literal(value: Any) -> str:
        return DeterministicCalendarQueryExecutor._literal(value)

    def _filter_sql(self, table_name: str, item: dict) -> str:
        column = f"{self._quote(table_name)}.{self._quote(item['column'])}"
        operator = item["operator"]
        if operator == "is_null":
            return f"{column} IS NULL"
        if operator == "is_not_null":
            return f"{column} IS NOT NULL"
        if operator in {"in", "not_in"}:
            values = ", ".join(self._literal(value) for value in item["value"])
            keyword = "IN" if operator == "in" else "NOT IN"
            return f"{column} {keyword} ({values})"
        sql_operator = {
            "eq": "=", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=",
        }[operator]
        return f"{column} {sql_operator} {self._literal(item['value'])}"

    def _grain(self, question: str, time_match: dict) -> Optional[tuple[str, str]]:
        grain_question = CalendarFilterCompiler._FISCAL_QUARTER_RE.sub(" ", question)
        grain_question = CalendarFilterCompiler._FISCAL_YEAR_RE.sub(" ", grain_question)
        explicit = {
            grain for grain, pattern in self._GRAIN_PATTERNS.items()
            if pattern.search(grain_question)
        }
        label = re.search(
            r"时间粒度\s*[:：]\s*(日|天|周|月|季度|季|年)",
            grain_question,
            re.IGNORECASE,
        )
        if label:
            explicit.add({
                "日": "day", "天": "day", "周": "week", "月": "month",
                "季度": "quarter", "季": "quarter", "年": "year",
            }[label.group(1)])
        if len(explicit) > 1:
            return None
        if explicit:
            return next(iter(explicit)), "explicit"
        default = str(time_match.get("default_grain") or "")
        if default in SemanticCatalog.TIME_GRAINS and self._TREND_SIGNAL_RE.search(question):
            return default, "semantic_default"
        return None

    def _day_expression(self, time_match: dict) -> Optional[tuple[str, dict]]:
        table = self.schema.tables.get(time_match["table"])
        if table is None:
            return None
        column = next(
            (item for item in table.columns if item.name == time_match["column"]), None,
        )
        if column is None:
            return None
        declared_type = re.sub(r"\s+", " ", str(column.type or "").strip()).upper()
        qualified = f"{self._quote(time_match['table'])}.{self._quote(time_match['column'])}"
        if declared_type == "DATE":
            return f"date({qualified})", {
                "storage_basis": "declared_date",
                "storage_basis_source": "inferred_schema",
                "timezone": None,
                "timezone_conversion": "none",
                "business_utc_offset_minutes": None,
                "tzdata_version": None,
                "iana_version": None,
            }
        if declared_type not in {"DATETIME", "TIMESTAMP"}:
            return None
        calendars = [
            entry for entry in self.semantic_catalog.entries
            if entry.get("kind") == "business_calendar"
            and entry.get("table") == time_match["table"]
            and entry.get("column") == time_match["column"]
        ]
        if len(calendars) != 1:
            return None
        calendar = calendars[0]["calendar"]
        day_expr = self.calendar_compiler._day_expression(time_match, calendar)
        if day_expr is None:
            return None
        basis = str(calendar.get("storage_basis") or "unspecified")
        return day_expr, {
            "storage_basis": basis,
            "storage_basis_source": calendar.get("storage_basis_source") or "legacy_default",
            "timezone": calendar.get("timezone"),
            "timezone_conversion": calendar.get("timezone_conversion") or "none",
            "business_utc_offset_minutes": calendar.get("business_utc_offset_minutes"),
            "tzdata_version": calendar.get("tzdata_version"),
            "iana_version": calendar.get("iana_version"),
        }

    def _bucket_expression(
        self,
        day_expr: str,
        grain: str,
        week_start_iso: int = 1,
    ) -> str:
        if grain == "day":
            return day_expr
        if grain == "week":
            sqlite_week_start = 0 if week_start_iso == 7 else week_start_iso
            weekday = (
                f"(CAST(strftime('%w', {day_expr}) AS INTEGER) - "
                f"{sqlite_week_start} + 7) % 7"
            )
            return f"date({day_expr}, printf('-%d days', {weekday}))"
        if grain == "month":
            return f"strftime('%Y-%m', {day_expr})"
        if grain == "quarter":
            quarter = (
                f"CAST(((CAST(strftime('%m', {day_expr}) AS INTEGER) - 1) / 3 + 1) AS INTEGER)"
            )
            return f"strftime('%Y', {day_expr}) || '-Q' || {quarter}"
        if grain == "year":
            return f"strftime('%Y', {day_expr})"
        raise ValueError("不支持的时间粒度")

    def _measures(
        self,
        question: str,
        matches: List[dict],
        table_name: str,
    ) -> Optional[tuple[List[str], List[dict], List[dict], bool]]:
        metrics = [
            item for item in matches
            if item.get("kind") == "metric" and item.get("table") == table_name
        ]
        if len(metrics) > DeterministicMultiMetricQueryExecutor.MAX_MEASURES:
            return None
        if not metrics:
            if not self._COUNT_MEASURE_RE.search(question):
                return None
            return ["COUNT(*)"], [{
                "term": "记录数", "aggregation": "count", "column": "", "filters": [],
            }], [], True
        metrics = DeterministicMultiMetricQueryExecutor._question_order(question, metrics)
        isolate_filters = len(metrics) > 1
        expressions: List[str] = []
        measures: List[dict] = []
        sql_filters: List[dict] = []
        for metric in metrics:
            expression_metric = metric
            if not isolate_filters:
                expression_metric = {**metric, "filters": []}
            expression = DeterministicMultiMetricQueryExecutor._measure_expression(
                self, table_name, expression_metric,
            )
            if expression is None:
                return None
            filters = [dict(item) for item in metric.get("filters") or []]
            expressions.append(expression)
            measures.append({
                "term": metric["term"],
                "aggregation": metric["aggregation"],
                "column": str(metric.get("column") or ""),
                "filters": filters,
            })
            if not isolate_filters:
                sql_filters.extend(filters)
        return expressions, measures, sql_filters, False

    @staticmethod
    def _date_range(question: str) -> Optional[dict]:
        matched = CalendarFilterCompiler._DATE_RANGE_RE.search(question)
        if not matched:
            return None
        try:
            start = date.fromisoformat(matched.group("start"))
            end = date.fromisoformat(matched.group("end"))
        except ValueError:
            return None
        if start > end:
            return None
        return {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "end_inclusive": True,
            "source": "explicit_range",
        }

    def _relative_date_range(self, question: str) -> Optional[dict]:
        matches = list(self._RELATIVE_RANGE_RE.finditer(question))
        if len(matches) != 1:
            return None
        matched = matches[0]
        count = int(matched.group("count"))
        unit = matched.group("unit")
        days = count if unit in {"天", "日"} else count * 7
        if days > self.MAX_RELATIVE_DAYS:
            return None

        references = list(self._REFERENCE_DATE_RE.finditer(question))
        if len(references) > 1:
            return None
        if references:
            try:
                reference = date.fromisoformat(references[0].group("date"))
            except ValueError:
                return None
            reference_source = "explicit_anchor"
        else:
            reference = self.reference_date or date.today()
            reference_source = "injected_reference" if self.reference_date else "runtime_local_date"
        start = reference - timedelta(days=days - 1)
        return {
            "start": start.isoformat(),
            "end": reference.isoformat(),
            "end_inclusive": True,
            "source": "relative",
            "expression": matched.group(0),
            "unit": "day" if unit in {"天", "日"} else "week",
            "count": count,
            "days": days,
            "reference_date": reference.isoformat(),
            "reference_source": reference_source,
        }

    def _supported_shape(
        self,
        question: str,
        matches: List[dict],
        time_match: dict,
        measure_term: str,
        is_count: bool,
    ) -> bool:
        remaining = CalendarFilterCompiler._DATE_RANGE_RE.sub(" ", str(question))
        remaining = CalendarFilterCompiler._FISCAL_QUARTER_RE.sub(" ", remaining)
        remaining = CalendarFilterCompiler._FISCAL_YEAR_RE.sub(" ", remaining)
        remaining = CalendarFilterCompiler._WORKDAY_RE.sub(" ", remaining)
        remaining = self._RELATIVE_RANGE_RE.sub(" ", remaining)
        remaining = self._REFERENCE_DATE_RE.sub(" ", remaining)
        for item in matches:
            remaining = re.sub(
                re.escape(str(item.get("term") or "")), " ", remaining, flags=re.IGNORECASE,
            )
        for token in (
            f"{time_match['table']}.{time_match['column']}",
            time_match["table"],
            time_match["column"],
        ):
            remaining = re.sub(re.escape(token), " ", remaining, flags=re.IGNORECASE)
        if is_count:
            remaining = self._COUNT_MEASURE_RE.sub(" ", remaining)
        else:
            remaining = re.sub(re.escape(measure_term), " ", remaining, flags=re.IGNORECASE)
        for pattern in self._GRAIN_PATTERNS.values():
            remaining = pattern.sub(" ", remaining)
        remaining = self._GRAIN_LABEL_RE.sub(" ", remaining)
        remaining = self._TREND_SIGNAL_RE.sub(" ", remaining)
        remaining = re.sub(
            r"(?:请帮我|帮我|麻烦|请|统计|计算|查询|查一下|一下|汇总|聚合|查看|展示|显示|分别|从|到|至|按|内|期间|之间|范围内|范围|"
            r"时间字段|时间范围|时间粒度|业务日历|指标口径|聚合字段|目标表|目标字段|新增内容|关联条件)",
            " ", remaining, flags=re.IGNORECASE,
        )
        remaining = re.sub(
            r"(?:以及|并且|而且|同时|还有|和|与|及)", " ", remaining,
            flags=re.IGNORECASE,
        )
        remaining = re.sub(r"[\s，,。！？!?；;：:、·的]+", "", remaining)
        return not remaining

    def answer(self, question: str, semantic: SemanticResolution) -> Optional[DBAnswer]:
        dialect = str(getattr(self.connector, "dialect", "sqlite") or "sqlite").lower()
        if dialect != "sqlite":
            return None
        time_matches = [item for item in semantic.matches if item.get("kind") == "time_field"]
        if len(time_matches) != 1:
            return None
        time_match = time_matches[0]
        table_name = str(time_match.get("table") or "")
        if any(
            item.get("kind") in {"ratio_metric", "dimension", "column_alias"}
            for item in semantic.matches
        ):
            return None
        if any(
            item.get("kind") in {
                "table_alias", "enum_value", "metric", "ratio_metric", "dimension",
                "time_field", "business_calendar", "column_alias",
            }
            and item.get("table") != table_name
            for item in semantic.matches
        ):
            return None
        grain_info = self._grain(question, time_match)
        if grain_info is None:
            return None
        grain, grain_source = grain_info
        day_info = self._day_expression(time_match)
        if day_info is None:
            return None
        day_expr, time_rules = day_info
        measure_result = self._measures(question, semantic.matches, table_name)
        if measure_result is None:
            return None
        expressions, measure_infos, sql_filters, is_count = measure_result
        global_filters = [{
            "column": item["column"], "operator": "eq", "value": item["value"],
        } for item in semantic.matches if item.get("kind") == "enum_value"]
        if len(measure_infos) > 1 and len(global_filters) > 1:
            return None
        sql_filters.extend(global_filters)
        explicit_range = self._date_range(question)
        if CalendarFilterCompiler._DATE_RANGE_RE.search(question) and explicit_range is None:
            return None
        relative_signal = self._RELATIVE_SIGNAL_RE.search(question)
        relative_range = self._relative_date_range(question) if relative_signal else None
        if relative_signal and relative_range is None:
            return None
        if self._REFERENCE_DATE_RE.search(question) and relative_range is None:
            return None

        calendar_plan = self.calendar_compiler.compile(question, semantic)
        calendar_signal = bool(
            CalendarFilterCompiler._FISCAL_QUARTER_RE.search(question)
            or CalendarFilterCompiler._FISCAL_YEAR_RE.search(question)
            or CalendarFilterCompiler._WORKDAY_RE.search(question)
        )
        if calendar_signal and calendar_plan is None:
            return None
        if calendar_plan is not None and relative_range is not None:
            return None
        if calendar_plan is None and explicit_range is not None and relative_range is not None:
            return None

        if calendar_plan is not None:
            date_range = {
                **calendar_plan.date_range,
                "source": "business_calendar",
                "calendar_term": calendar_plan.calendar_term,
                "calendar_mode": calendar_plan.mode,
            }
        else:
            date_range = relative_range or explicit_range
        if not self._supported_shape(
            question, semantic.matches, time_match, measure_infos[0]["term"], is_count,
        ):
            return None

        week_start_iso = (
            int(calendar_plan.rules.get("week_start_iso") or 1)
            if calendar_plan is not None else 1
        )
        bucket_expr = self._bucket_expression(day_expr, grain, week_start_iso)
        bucket_label = self._BUCKET_LABELS[grain]
        predicates = []
        if calendar_plan is not None:
            predicates.append(calendar_plan.predicate)
        else:
            predicates.append(f"{day_expr} IS NOT NULL")
        if date_range and calendar_plan is None:
            predicates.extend([
                f"{day_expr} >= {self._literal(date_range['start'])}",
                f"{day_expr} <= {self._literal(date_range['end'])}",
            ])
        predicates.extend(self._filter_sql(table_name, item) for item in sql_filters)
        select_measures = [
            f"{expression} AS {self._quote(measure_info['term'])}"
            for expression, measure_info in zip(expressions, measure_infos)
        ]
        sql = (
            f"SELECT {bucket_expr} AS {self._quote(bucket_label)}, "
            + ", ".join(select_measures) + " "
            f"FROM {self._quote(table_name)} "
            + "WHERE " + " AND ".join(f"({item})" for item in predicates)
            + f" GROUP BY {bucket_expr} ORDER BY {bucket_expr}"
        )
        plan = TrendAggregatePlan(
            table=table_name,
            column=time_match["column"],
            time_term=time_match["term"],
            grain=grain,
            grain_source=grain_source,
            bucket={"label": bucket_label, "expression": bucket_expr},
            measure=measure_infos[0],
            measures=measure_infos,
            filters=[
                *[item for measure in measure_infos for item in measure.get("filters") or []],
                *global_filters,
            ],
            global_filters=global_filters,
            date_range=date_range,
            rules={
                **time_rules,
                **(calendar_plan.rules if calendar_plan is not None else {}),
                "week_start_iso": week_start_iso,
                "invalid_dates": "excluded",
                "time_window_source": (
                    date_range.get("source") if date_range else "all_valid_dates"
                ),
                "reference_date": (
                    date_range.get("reference_date") if date_range else None
                ),
                "reference_source": (
                    date_range.get("reference_source") if date_range else None
                ),
                "calendar_term": (
                    calendar_plan.calendar_term if calendar_plan is not None else None
                ),
                "calendar_mode": (
                    calendar_plan.mode if calendar_plan is not None else None
                ),
            },
        )
        result = self.security.execute(sql)
        plan.sql = result.sql
        if result.error:
            plan.status = "failed"
            return DBAnswer(
                kind="error",
                narrative=f"确定性时间趋势查询失败：{result.error}",
                sql=result.sql,
                error=result.error,
                trend_plan=plan.as_dict(),
            )
        plan.status = "executed"
        range_text = ""
        if date_range:
            end_text = "（含首尾）" if date_range["end_inclusive"] else "（结束日不含）"
            range_text = f"，范围 {date_range['start']} 至 {date_range['end']}{end_text}"
        narrative = (
            f"已按{SemanticCatalog.TIME_GRAIN_LABELS[grain]}执行确定性趋势聚合："
            f"{time_match['term']}，指标为{'、'.join(item['term'] for item in measure_infos)}"
            f"{range_text}。"
        )
        return DBAnswer(
            kind="query",
            narrative=narrative,
            sql=result.sql,
            columns=list(result.columns),
            rows=result.rows,
            trend_plan=plan.as_dict(),
            steps=[{
                "tool": "trend_aggregate",
                "grain": grain,
                "measures": len(measure_infos),
                "dialect": plan.dialect,
                "status": plan.status,
            }],
        )


class WriteSecurityError(DBAgentError):
    """写 SQL 安全拦截（多语句/危险操作/无 WHERE 等）。"""
    status_code = 400


class WriteSecurity:
    """写 SQL 校验：单条 DML/DDL、UPDATE/DELETE 强制 WHERE、禁危险系统操作。

    只做语法/风险级校验；真正落库必须经过 dry-run 预览 + 用户确认。
    """

    # 允许的操作前缀（DML + DDL，用户已确认支持 DDL）
    ALLOWED_KINDS = ("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP")

    # 写路径中禁止出现的系统级关键字（即使混在允许语句里）
    FORBIDDEN = re.compile(
        r"\b(PRAGMA|ATTACH|DETACH|VACUUM|REINDEX|TRIGGER|SAVEPOINT|RELEASE|"
        r"BEGIN|COMMIT|ROLLBACK|EXPLAIN)\b",
        re.IGNORECASE,
    )

    def __init__(
        self,
        timeout_s: float = 15.0,
        allowed_tables: Optional[List[str]] = None,
        allowed_columns: Optional[Dict[str, List[str]]] = None,
        row_filters: Optional[Dict[str, List[dict]]] = None,
    ):
        self.timeout_s = timeout_s
        self.allowed_tables = (
            frozenset(str(name).casefold() for name in allowed_tables)
            if allowed_tables is not None else None
        )
        self.allowed_columns = _normalize_column_scope(allowed_columns)
        if self.allowed_columns and self.allowed_tables is None:
            raise ValueError("字段级授权必须建立在显式表级授权之上")

        self.row_filters = _normalize_row_scope(row_filters)
        if self.row_filters and self.allowed_tables is None:
            raise ValueError("row-level authorization requires an explicit table scope")

    def validate_write(self, sql: str) -> dict:
        """校验写 SQL，返回 {kind, table, dangerous}；不合法抛 WriteSecurityError。"""
        if self.row_filters:
            raise WriteSecurityError("行级授权凭据当前只允许只读操作")
        s, code = _normalize_single_sql_statement(
            sql, WriteSecurityError, "仅支持单条 SQL 语句（不支持多语句/带分号）",
        )
        visible = code.strip()
        m = self.FORBIDDEN.search(visible)
        if m:
            raise WriteSecurityError(f"写操作禁止包含系统级关键字: {m.group(0)}")
        upper = visible.upper()
        kind = None
        for k in self.ALLOWED_KINDS:
            if upper.startswith(k):
                kind = k
                break
        if kind is None:
            raise WriteSecurityError("仅支持 INSERT/UPDATE/DELETE/CREATE/ALTER/DROP")
        if kind in ("UPDATE", "DELETE") and not re.search(r"\bWHERE\b", visible, re.IGNORECASE):
            raise WriteSecurityError(f"{kind} 语句必须带 WHERE 条件，禁止无界更新/删除")
        table = self._extract_table(kind, s)
        if self.allowed_tables is not None:
            if kind in ("CREATE", "ALTER", "DROP"):
                raise WriteSecurityError("表级授权凭据不允许执行 DDL")
            if not table or table.casefold() not in self.allowed_tables:
                raise WriteSecurityError("写操作目标表不在当前凭据授权范围内")
        scoped_columns = self.allowed_columns.get(table.casefold()) if table else None
        if scoped_columns is not None:
            if kind == "DELETE":
                raise WriteSecurityError("字段级授权凭据不能删除整行记录")
            if kind == "INSERT":
                target_columns = self._extract_insert_columns(s)
                if target_columns is None:
                    raise WriteSecurityError("字段级授权下 INSERT 必须显式列出目标字段")
                if any(column.casefold() not in scoped_columns for column in target_columns):
                    raise WriteSecurityError("INSERT 包含当前凭据未授权的目标字段")
        return {
            "kind": kind,
            "table": table,
            "dangerous": kind in ("CREATE", "ALTER", "DROP"),
        }

    @staticmethod
    def _extract_table(kind: str, sql: str) -> str:
        """粗略提取主表名（用于展示/确认卡片，无需 100% 精确）。"""
        s = sql.strip()
        try:
            if kind == "INSERT":
                m = re.search(r"\bINTO\s+([\"'`]?)([\w$]+)\1", s, re.I)
            elif kind == "UPDATE":
                m = re.search(r"\bUPDATE\s+(?:OR\s+\w+\s+)?([\"'`]?)([\w$]+)\1", s, re.I)
            elif kind == "DELETE":
                m = re.search(r"\bFROM\s+([\"'`]?)([\w$]+)\1", s, re.I)
            elif kind == "ALTER":
                m = re.search(r"\bTABLE\s+([\"'`]?)([\w$]+)\1", s, re.I)
            else:  # CREATE / DROP
                m = re.search(r"\b(?:TABLE|INDEX|VIEW)\s+([\"'`]?)([\w$]+)\1", s, re.I)
            return m.group(2) if m else ""
        except Exception:
            return ""

    @staticmethod
    def _extract_insert_columns(sql: str) -> Optional[List[str]]:
        match = re.search(
            r"\bINTO\s+(?:\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|[\w$]+)\s*"
            r"\((?P<columns>[^)]*)\)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if match is None:
            return None
        columns = []
        for raw in match.group("columns").split(","):
            token = raw.strip()
            if not token:
                return None
            if token.startswith('"') and token.endswith('"'):
                token = token[1:-1].replace('""', '"')
            elif token.startswith("`") and token.endswith("`"):
                token = token[1:-1].replace("``", "`")
            elif token.startswith("[") and token.endswith("]"):
                token = token[1:-1].replace("]]", "]")
            elif not re.fullmatch(r"[\w$]+", token):
                return None
            if not token:
                return None
            columns.append(token)
        return columns


@dataclass
class WriteProposal:
    """一条待确认的写操作提案（dry-run 预览后生成，用户批准才落库）。"""

    confirm_id: str
    sql: str
    kind: str                  # INSERT/UPDATE/DELETE/CREATE/ALTER/DROP
    table: str
    summary_zh: str
    dangerous: bool            # DDL 或高风险操作标记
    preview: dict              # 前后对比/影响行数（WritePreviewer 产出）
    db_path: str
    created_at: float = field(default_factory=time.time)
    approved: Optional[bool] = None


class WriteConfirmationRegistry:
    """写操作确认注册表（Human-in-the-loop，本地一次性确认协议）。

    流程：NL2WriteExecutor.prepare → register() 挂起等待用户
        → confirm_write() approve/reject 一次性取出
        → cleanup_expired() 超时(默认300s)自动清理
    """

    def __init__(self, ttl: float = 300.0):
        self.ttl = ttl
        self._pending: Dict[str, WriteProposal] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _same_database(left: str, right: str) -> bool:
        if "://" in str(left) or "://" in str(right):
            return str(left).rstrip("/") == str(right).rstrip("/")
        return os.path.normcase(str(Path(left).resolve())) == os.path.normcase(str(Path(right).resolve()))

    def _cleanup_expired_locked(self) -> int:
        now = time.time()
        expired = [cid for cid, p in self._pending.items() if now - p.created_at > self.ttl]
        for cid in expired:
            self._pending.pop(cid, None)
        return len(expired)

    def register(self, proposal: WriteProposal) -> str:
        with self._lock:
            self._cleanup_expired_locked()
            self._pending[proposal.confirm_id] = proposal
            return proposal.confirm_id

    def get(self, confirm_id: str) -> Optional[WriteProposal]:
        with self._lock:
            self._cleanup_expired_locked()
            return self._pending.get(confirm_id)

    def resolve(self, confirm_id: str, expected_db_path: Optional[str] = None) -> Optional[WriteProposal]:
        """一次性取出；指定数据库时，确认单必须属于该数据库。"""
        with self._lock:
            self._cleanup_expired_locked()
            proposal = self._pending.get(confirm_id)
            if proposal is None:
                return None
            if expected_db_path and not self._same_database(proposal.db_path, expected_db_path):
                return None
            return self._pending.pop(confirm_id, None)

    def cleanup_expired(self) -> int:
        with self._lock:
            return self._cleanup_expired_locked()

    def stats(self) -> dict:
        with self._lock:
            self._cleanup_expired_locked()
            return {"pending": len(self._pending), "ttl_s": self.ttl}


# 模块级单例：bridge 与 DBAgent 共享同一确认注册表
WRITE_REGISTRY = WriteConfirmationRegistry()


class WritePreviewer:
    """写操作 dry-run 预览：rw 连接上事务内执行 → 采集前后对比 → ROLLBACK，零副作用。

    DML(INSERT/UPDATE/DELETE)：先 SELECT 命中行(前值) → 事务内执行 → SELECT 受影响行(后值) → 回滚
    DDL(CREATE/ALTER/DROP)：事务内执行 → 采集变更后结构(sqlite_master/PRAGMA table_info) → 回滚
    （SQLite 的 DDL 是事务性的，同样可安全回滚）
    """

    def __init__(
        self,
        connector: DBConnector,
        max_preview_rows: int = 20,
        allowed_tables: Optional[List[str]] = None,
        allowed_columns: Optional[Dict[str, List[str]]] = None,
    ):
        self.connector = connector
        self.max_preview_rows = max_preview_rows
        self.allowed_tables = (
            frozenset(str(name).casefold() for name in allowed_tables)
            if allowed_tables is not None else None
        )
        self.allowed_columns = _normalize_column_scope(allowed_columns)
        if self.allowed_columns and self.allowed_tables is None:
            raise ValueError("字段级授权必须建立在显式表级授权之上")

    def _install_table_authorizer(self, conn: sqlite3.Connection) -> None:
        _install_sqlite_scope_authorizer(
            conn,
            allowed_tables=self.allowed_tables,
            allowed_columns=self.allowed_columns,
            allow_writes=True,
            unavailable_error="当前连接无法安全执行表/字段级授权写入",
        )

    def preview(self, sql: str, meta: dict) -> dict:
        """执行 dry-run，返回前后对比预览数据（不落库）。"""
        kind = meta["kind"]
        table = meta.get("table") or ""
        conn = self.connector.connect_rw()
        try:
            self._install_table_authorizer(conn)
            if kind in ("INSERT", "UPDATE", "DELETE"):
                return self._preview_dml(conn, sql, kind, table)
            return self._preview_ddl(conn, sql, kind, table)
        finally:
            self.connector.close(conn)

    # ---- DML ----
    def _preview_dml(self, conn, sql, kind, table):
        where = self._extract_where(sql)
        projection = self._preview_projection(table)
        before = {"columns": [], "rows": []}
        if kind in ("UPDATE", "DELETE") and where and table:
            try:
                cur = conn.execute(
                    f'SELECT {projection} FROM "{table}" WHERE {where} '
                    f'LIMIT {self.max_preview_rows + 1}'
                )
                before = self._collect(cur)
            except Exception:
                before = {"columns": [], "rows": []}

        conn.execute("BEGIN")
        try:
            cur = conn.execute(sql)
            rowcount = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0

            after = {"columns": [], "rows": []}
            if kind == "UPDATE" and where and table:
                cur = conn.execute(
                    f'SELECT {projection} FROM "{table}" WHERE {where} '
                    f'LIMIT {self.max_preview_rows + 1}'
                )
                after = self._collect(cur)
            elif kind == "INSERT" and table and table.casefold() not in self.allowed_columns:
                try:
                    cur = conn.execute(
                        f'SELECT * FROM "{table}" WHERE rowid = ?', (cur.lastrowid,)
                    )
                    after = self._collect(cur)
                except Exception:
                    cur = conn.execute(f'SELECT * FROM "{table}" ORDER BY rowid DESC LIMIT 1')
                    after = self._collect(cur)
            # DELETE: after 保持空（行已删）
        finally:
            conn.execute("ROLLBACK")

        return {
            "kind": kind,
            "table": table,
            "affected": rowcount,
            "before": before,
            "after": after,
            "ddl": None,
        }

    # ---- DDL ----
    def _preview_ddl(self, conn, sql, kind, table):
        before = self._describe(conn, table)
        conn.execute("BEGIN")
        try:
            conn.execute(sql)
            after = self._describe(conn, table)
        finally:
            conn.execute("ROLLBACK")
        return {
            "kind": kind,
            "table": table,
            "affected": 0,
            "before": before,
            "after": after,
            "ddl": {"before": before, "after": after},
        }

    # ---- utils ----
    def _collect(self, cur):
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = [self._safe_row(r) for r in cur.fetchmany(self.max_preview_rows + 1)]
        truncated = len(rows) > self.max_preview_rows
        return {"columns": cols, "rows": rows[: self.max_preview_rows], "truncated": truncated}

    @staticmethod
    def _safe_row(row):
        out = []
        for v in row:
            if isinstance(v, bytes):
                out.append(f"<blob {len(v)}B>")
            elif isinstance(v, (str, int, float, bool)) or v is None:
                out.append(v)
            else:
                out.append(str(v))
        return out

    @staticmethod
    def _extract_where(sql: str) -> str:
        m = re.search(r"\bWHERE\b(.*)$", sql, re.IGNORECASE | re.DOTALL)
        return m.group(1).strip() if m else ""

    def _preview_projection(self, table: str) -> str:
        columns = self.allowed_columns.get(str(table or "").casefold())
        if columns is None:
            return "*"
        return ", ".join(
            '"' + column.replace('"', '""') + '"'
            for column in sorted(columns)
        )

    def _describe(self, conn, table):
        """采集表/视图/索引定义 + 列结构（DROP 后 exists=False）。"""
        if not table:
            return {"sql": None, "columns": [], "exists": False}
        try:
            cur = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type IN ('table','index','view') AND name=?",
                (table,),
            )
            row = cur.fetchone()
            cols = []
            exists = False
            try:
                cur = conn.execute(f'PRAGMA table_info("{table}")')
                cols = [{"name": r[1], "type": r[2]} for r in cur.fetchall()]
                exists = True
            except Exception:
                pass
            return {"sql": row[0] if row else None, "columns": cols, "exists": exists}
        except Exception:
            return {"sql": None, "columns": [], "exists": False}


def _prepare_write_proposal(
    connector: DBConnector,
    security: WriteSecurity,
    previewer: WritePreviewer,
    sql: str,
    summary: str,
) -> DBAnswer:
    """Validate and preview one write, then register a one-time proposal.

    Natural-language writes and structured form inserts share this exact
    safety boundary.  No caller can create a confirmable proposal without the
    single-statement validator and rollback-only preview.
    """
    try:
        meta = security.validate_write(sql)
    except WriteSecurityError as exc:
        return DBAnswer(
            kind="error",
            narrative=f"写操作被安全拦截：{exc}",
            error=str(exc),
        )
    try:
        preview = previewer.preview(sql, meta)
    except Exception as exc:  # noqa: BLE001 -- preview failure never mutates
        return DBAnswer(
            kind="error",
            narrative=f"写操作预览失败，未执行任何写入：{exc}",
            error=str(exc),
        )
    if not summary:
        action_label = {
            "INSERT": "新增数据", "UPDATE": "更新数据", "DELETE": "删除数据",
            "CREATE": "创建对象", "ALTER": "修改结构", "DROP": "删除对象",
        }.get(str(meta.get("kind") or "").upper(), "数据库变更")
        summary = f"{action_label}：{meta.get('table') or '目标对象'}"
    proposal = WriteProposal(
        confirm_id=uuid.uuid4().hex[:16],
        sql=sql,
        kind=meta["kind"],
        table=meta["table"],
        summary_zh=summary,
        dangerous=meta["dangerous"],
        preview=preview,
        db_path=connector.db_path,
    )
    WRITE_REGISTRY.register(proposal)
    return DBAnswer(
        kind="write_pending",
        narrative=proposal.summary_zh,
        sql=sql,
        confirm_id=proposal.confirm_id,
        write=asdict(proposal),
    )


class StructuredInsertWorkflow:
    """Schema-bound single-row INSERT form with no model-generated SQL.

    The browser receives only authorized schema fields and one row fetched
    through the ordinary read-only security layer.  Submitted cells are
    converted to typed literals locally, previewed in a rolled-back
    transaction, and still require the existing explicit confirmation.
    """

    _INTEGER_RE = re.compile(r"^[+-]?\d+$")
    _REAL_RE = re.compile(
        r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$"
    )
    def __init__(
        self,
        connector: DBConnector,
        schema: SchemaSnapshot,
        read_security: SQLSecurity,
        write_security: WriteSecurity,
        previewer: WritePreviewer,
    ):
        self.connector = connector
        self.schema = schema
        self.read_security = read_security
        self.write_security = write_security
        self.previewer = previewer

    @staticmethod
    def should_offer(
        plan: DatabaseOperationPlan,
        clarification: Optional[dict],
        intent_result: IntentResult,
    ) -> bool:
        """Route model-classified incomplete inserts to the schema-bound form."""
        if plan.mode != "write" or plan.action != "insert" or not clarification:
            return False
        if intent_result.intent != "write" or intent_result.interaction != "guided_insert":
            return False
        missing = str(clarification.get("missing") or "")
        return missing in {"target_table", "record_values"}

    def _ensure_available(self) -> None:
        if not isinstance(self.connector, DBConnector):
            raise WriteSecurityError("表格式写入目前只对已验证的本地 SQLite 数据库开放")
        if self.write_security.row_filters:
            raise WriteSecurityError("行级授权凭据固定为只读，不能打开写入表单")

    def _table(self, table_name: str) -> Optional[DBTable]:
        name = str(table_name or "").strip()
        if not name:
            return None
        matches = [
            table for table in self.schema.tables.values()
            if table.name.casefold() == name.casefold()
        ]
        if len(matches) != 1:
            raise WriteSecurityError("目标表不存在或不在当前授权范围内")
        return matches[0]

    @staticmethod
    def _quoted_identifier(name: str) -> str:
        return '"' + str(name).replace('"', '""') + '"'

    @staticmethod
    def _auto_integer_primary_key(table: DBTable, column: DBColumn) -> bool:
        primary_keys = [item for item in table.columns if item.pk]
        return bool(
            len(primary_keys) == 1
            and column.pk
            and str(column.type or "").strip().upper() == "INTEGER"
        )

    def _column_spec(self, table: DBTable, column: DBColumn) -> dict:
        automatic = self._auto_integer_primary_key(table, column)
        has_default = bool(column.default_sql)
        required = bool(not column.nullable and not has_default and not automatic)
        return {
            "name": column.name,
            "type": column.type or "TEXT",
            "nullable": bool(column.nullable),
            "primary_key": bool(column.pk),
            "automatic": automatic,
            "has_default": has_default,
            "default_sql": column.default_sql,
            "required": required,
        }

    def form(self, table_name: str = "") -> DBAnswer:
        self._ensure_available()
        table = self._table(table_name)
        tables = [
            {"name": item.name, "row_count": item.row_count}
            for item in self.schema.tables.values()
        ]
        payload = {
            "mode": "insert_form",
            "version": "1.0",
            "tables": tables,
            "selected_table": table.name if table else "",
            "columns": [],
            "example": {"columns": [], "rows": []},
        }
        if table is not None:
            payload["columns"] = [
                self._column_spec(table, column) for column in table.columns
            ]
            if table.columns:
                projection = ", ".join(
                    self._quoted_identifier(column.name) for column in table.columns
                )
                sql = (
                    f"SELECT {projection} FROM "
                    f"{self._quoted_identifier(table.name)} LIMIT 1"
                )
                sample = self.read_security.execute(sql)
                if sample.error:
                    raise WriteSecurityError(f"读取示例行失败：{sample.error}")
                payload["example"] = {
                    "columns": list(sample.columns),
                    "rows": [
                        WritePreviewer._safe_row(row) for row in sample.rows[:1]
                    ],
                }
        narrative = (
            f"请选择要写入的表，并参照原表中的一行填写新记录。"
            if table is None else
            f"请参照 {table.name} 的示例行填写一条新记录。"
        )
        return DBAnswer(
            kind="write_form",
            narrative=narrative,
            write=payload,
            steps=[{
                "tool": "structured_insert_form",
                "version": "1.0",
                "status": "awaiting_input",
                "model_calls": 0,
                "table": table.name if table else "",
            }],
        )

    @classmethod
    def _typed_value(cls, column: DBColumn, raw: Any) -> Any:
        if isinstance(raw, (dict, list)):
            raise WriteSecurityError(f"字段 {column.name} 的值类型无效")
        value = "" if raw is None else str(raw)
        declared = str(column.type or "").upper()
        if "BLOB" in declared:
            raise WriteSecurityError(f"字段 {column.name} 是 BLOB，当前表单不支持二进制写入")
        if "BOOL" in declared:
            folded = value.strip().casefold()
            if folded in {"1", "true", "yes", "是"}:
                return 1
            if folded in {"0", "false", "no", "否"}:
                return 0
            raise WriteSecurityError(f"字段 {column.name} 需要布尔值")
        if "INT" in declared:
            if not cls._INTEGER_RE.fullmatch(value.strip()):
                raise WriteSecurityError(f"字段 {column.name} 需要整数")
            return int(value.strip())
        if any(token in declared for token in ("REAL", "FLOA", "DOUB", "NUMERIC", "DECIMAL")):
            if not cls._REAL_RE.fullmatch(value.strip()):
                raise WriteSecurityError(f"字段 {column.name} 需要数值")
            number = float(value.strip())
            if not math.isfinite(number):
                raise WriteSecurityError(f"字段 {column.name} 需要有限数值")
            return number
        return value

    def prepare(self, table_name: str, fields: Any) -> DBAnswer:
        self._ensure_available()
        table = self._table(table_name)
        if table is None:
            raise WriteSecurityError("请选择目标表")
        if not isinstance(fields, list) or len(fields) > 512:
            raise WriteSecurityError("写入字段必须是有界结构化列表")

        by_name = {column.name.casefold(): column for column in table.columns}
        submitted: Dict[str, dict] = {}
        for item in fields:
            if not isinstance(item, dict):
                raise WriteSecurityError("写入字段格式无效")
            raw_name = str(item.get("column") or "").strip()
            column = by_name.get(raw_name.casefold())
            if column is None:
                raise WriteSecurityError("写入包含不存在或未授权的字段")
            key = column.name.casefold()
            if key in submitted:
                raise WriteSecurityError(f"字段 {column.name} 重复提交")
            mode = str(item.get("mode") or "omit").strip().lower()
            if mode not in {"value", "null", "omit"}:
                raise WriteSecurityError(f"字段 {column.name} 的填写模式无效")
            submitted[key] = {"column": column, "mode": mode, "value": item.get("value")}

        target_columns: List[DBColumn] = []
        values: List[Any] = []
        for column in table.columns:
            item = submitted.get(column.name.casefold(), {"mode": "omit"})
            mode = item["mode"]
            automatic = self._auto_integer_primary_key(table, column)
            if mode == "omit":
                if not column.nullable and not column.default_sql and not automatic:
                    raise WriteSecurityError(f"字段 {column.name} 必填，不能使用默认值")
                continue
            if mode == "null":
                if not column.nullable and not automatic:
                    raise WriteSecurityError(f"字段 {column.name} 不允许 NULL")
                value = None
            else:
                value = self._typed_value(column, item.get("value"))
            target_columns.append(column)
            values.append(value)

        quoted_table = self._quoted_identifier(table.name)
        if target_columns:
            columns_sql = ", ".join(
                self._quoted_identifier(column.name) for column in target_columns
            )
            values_sql = ", ".join(_sqlite_literal(value) for value in values)
            sql = f"INSERT INTO {quoted_table} ({columns_sql}) VALUES ({values_sql})"
        else:
            sql = f"INSERT INTO {quoted_table} DEFAULT VALUES"
        return _prepare_write_proposal(
            self.connector,
            self.write_security,
            self.previewer,
            sql,
            f"向 {table.name} 新增一条记录",
        )


class StructuredCreateTableWorkflow:
    """Compile a bounded typed form into CREATE TABLE without model-written SQL."""

    ALLOWED_TYPES = (
        "INTEGER", "REAL", "NUMERIC", "TEXT", "BLOB", "BOOLEAN", "DATE", "DATETIME",
    )
    _INTEGER_RE = re.compile(r"^[+-]?\d+$")
    _REAL_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")

    def __init__(
        self,
        connector: DBConnector,
        schema: SchemaSnapshot,
        write_security: WriteSecurity,
        previewer: WritePreviewer,
    ):
        self.connector = connector
        self.schema = schema
        self.write_security = write_security
        self.previewer = previewer

    def _ensure_available(self) -> None:
        if not isinstance(self.connector, DBConnector):
            raise WriteSecurityError("自定义建表目前只对已验证的本地 SQLite 数据库开放")
        if self.write_security.allowed_tables is not None or self.write_security.row_filters:
            raise WriteSecurityError("受限范围凭据不允许创建数据库结构")

    @staticmethod
    def _identifier(raw: Any, label: str) -> str:
        name = str(raw or "").strip()
        if not name or len(name) > 64 or not name.isidentifier():
            raise WriteSecurityError(f"{label}必须是 1–64 位字母、数字、下划线或中文标识符，且不能以数字开头")
        if name.casefold().startswith("sqlite_"):
            raise WriteSecurityError(f"{label}不能使用 SQLite 保留前缀 sqlite_")
        return name

    @staticmethod
    def _quote(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    @classmethod
    def _default_sql(cls, column_type: str, mode: str, raw: Any, nullable: bool) -> str:
        if mode == "none":
            return ""
        if mode == "null":
            if not nullable:
                raise WriteSecurityError("必填字段不能把 NULL 设为默认值")
            return "NULL"
        if mode == "current_timestamp":
            if column_type not in {"TEXT", "DATE", "DATETIME"}:
                raise WriteSecurityError("仅 TEXT、DATE 或 DATETIME 字段可使用当前时间默认值")
            return "CURRENT_TIMESTAMP"
        if mode != "value":
            raise WriteSecurityError("默认值模式无效")
        value = "" if raw is None else str(raw)
        if len(value) > 500:
            raise WriteSecurityError("默认值最长为 500 个字符")
        if column_type == "BLOB":
            raise WriteSecurityError("BLOB 字段不支持表单默认值")
        if column_type == "INTEGER":
            if not cls._INTEGER_RE.fullmatch(value):
                raise WriteSecurityError("INTEGER 字段的默认值必须是整数")
            return str(int(value))
        if column_type in {"REAL", "NUMERIC"}:
            if not cls._REAL_RE.fullmatch(value):
                raise WriteSecurityError(f"{column_type} 字段的默认值必须是数值")
            number = float(value)
            if not math.isfinite(number):
                raise WriteSecurityError("数值默认值必须有限")
            return repr(number)
        if column_type == "BOOLEAN":
            folded = value.casefold()
            if folded in {"1", "true", "yes", "是"}:
                return "1"
            if folded in {"0", "false", "no", "否"}:
                return "0"
            raise WriteSecurityError("BOOLEAN 字段的默认值必须是 true/false 或 1/0")
        return _sqlite_literal(value)

    def prepare(self, table_name: str, columns: Any) -> DBAnswer:
        self._ensure_available()
        table = self._identifier(table_name, "表名")
        if table.casefold() in {name.casefold() for name in self.schema.tables}:
            raise WriteSecurityError(f"数据表 {table} 已存在")
        if not isinstance(columns, list) or not 1 <= len(columns) <= 64:
            raise WriteSecurityError("建表必须包含 1–64 个字段")

        definitions = []
        seen = set()
        primary_keys = 0
        for index, raw_column in enumerate(columns, start=1):
            if not isinstance(raw_column, dict):
                raise WriteSecurityError(f"第 {index} 个字段格式无效")
            name = self._identifier(raw_column.get("name"), f"第 {index} 个字段名")
            folded = name.casefold()
            if folded in seen:
                raise WriteSecurityError(f"字段 {name} 重复")
            seen.add(folded)
            column_type = str(raw_column.get("type") or "TEXT").strip().upper()
            if column_type not in self.ALLOWED_TYPES:
                raise WriteSecurityError(f"字段 {name} 的类型不受支持")
            primary_key = raw_column.get("primaryKey") is True
            auto_increment = raw_column.get("autoIncrement") is True
            nullable = raw_column.get("nullable") is not False
            unique = raw_column.get("unique") is True
            if primary_key:
                primary_keys += 1
                if primary_keys > 1:
                    raise WriteSecurityError("当前表单只支持一个主键字段")
            if auto_increment and (not primary_key or column_type != "INTEGER"):
                raise WriteSecurityError("自增只能用于 INTEGER 主键")
            default_mode = str(raw_column.get("defaultMode") or "none").strip().lower()
            if primary_key and default_mode != "none":
                raise WriteSecurityError("主键字段不能在此表单中设置默认值")
            default_sql = self._default_sql(
                column_type, default_mode, raw_column.get("defaultValue"), nullable,
            )
            parts = [self._quote(name), column_type]
            if primary_key:
                parts.append("PRIMARY KEY")
            if auto_increment:
                parts.append("AUTOINCREMENT")
            if not nullable:
                parts.append("NOT NULL")
            if unique and not primary_key:
                parts.append("UNIQUE")
            if default_sql:
                parts.extend(["DEFAULT", default_sql])
            definitions.append(" ".join(parts))

        sql = f"CREATE TABLE {self._quote(table)} (" + ", ".join(definitions) + ")"
        return _prepare_write_proposal(
            self.connector,
            self.write_security,
            self.previewer,
            sql,
            f"创建数据表 {table}（{len(definitions)} 个字段）",
        )


# ---------------------------------------------------------------------------
# NL2SQLExecutor —— LLM 生成 SQL → 安全执行 → 自纠错（步骤4填充）
# ---------------------------------------------------------------------------

class NL2SQLExecutor:
    """自然语言 → SQL → 执行 → 自纠错（最多2轮），输出 {摘要+SQL+表格}。"""

    _COUNTED_RELATIONSHIP_SUPERLATIVE_RE = re.compile(
        r"\b(?P<direction>most|least|fewest)\s+"
        r"(?:(?P<counter>number|count)\s+of\s+)?"
        r"(?:(?:departing|arriving)\s+)?"
        r"(?P<relation>children|people|men|women|[A-Za-z][\w-]*s)\b",
        re.IGNORECASE,
    )
    _GROUPED_RELATIONSHIP_COUNT_RE = re.compile(
        r"^\s*(?:what\s+are|show|list|return|give(?:\s+me)?)\s+"
        r"(?:the\s+)?(?P<label>names?|ids?|identifiers?|codes?)\s+of\s+"
        r"(?:the\s+)?(?P<entity>[A-Za-z][\w-]*"
        r"(?:\s+[A-Za-z][\w-]*){0,2}?)\s+and\s+how\s+many\s+"
        r"(?P<fact>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*){0,2}?)\s+"
        r"(?:do|does|did|have|has|had)\s+"
        r"(?:they|each|the\s+[A-Za-z][\w-]*)\s+"
        r"(?P<verb>teach|offer|have|own|manage|conduct|take|attend|lead|"
        r"supervise|use)\s*[?.。]?\s*$",
        re.IGNORECASE,
    )

    _SYSTEM_PROMPT = (
        "你是 {dialect_name} 数据库问答助手。根据下面的数据库结构，把用户问题翻译成一条只读 SQL 查询。\n"
        "规则：\n"
        "1. 只生成 SELECT 语句（可含 WITH/CTE/JOIN/聚合），严禁任何写操作关键字\n"
        "2. 表名/列名必须与 schema 完全一致；条件值优先使用 schema 中给出的抽样值\n"
        "3. SELECT 只返回回答问题所需的最少列；不要额外返回主键、状态或其他未请求字段。"
        "例如问‘哪些客户’只返回客户名称，问‘每位客户的金额’只返回客户名称和金额\n"
        "4. 优先生成最简单、直接且可验证的 SQL：仅在确有必要时使用 CTE、CAST、DISTINCT、LEFT JOIN；"
        "结果列无需 AS 别名；统计记录数优先 COUNT(*)；多表连接优先使用 schema 声明的外键\n"
        "5. 在写 SQL 前先明确查询合同：输出、每行粒度、筛选、分组、关联、排序与条数；"
        "SQL 必须逐项实现该合同\n"
        "6. 结果只输出一个 JSON 对象：{{\"intent\":{{"
        "\"outputs\":[\"按顺序的输出语义\"],"
        "\"row_grain\":\"single_value|single_row|detail_rows|one_row_per_entity|"
        "one_row_per_group|top_k|set_of_entities|unknown\","
        "\"filters\":[\"筛选口径\"],\"grouping\":[\"分组口径\"],"
        "\"relations\":[\"关联口径\"],\"ordering_limit\":\"排序与条数\"}},"
        "\"sql\":\"...\",\"summary_zh\":\"一句话中文摘要\"}}\n"
        "7. sql 字段内不要用 markdown 围栏\n\n"
        "数据库结构：\n{schema}\n\n"
        "用户问题：{question}\n\n"
        "输出 JSON："
    )

    _RETRY_PROMPT = (
        "你之前的 SQL 未通过执行或语义复核，请根据错误修正查询合同和 SQL 后重新输出 JSON"
        "（{{\"intent\":{{\"outputs\":[],\"row_grain\":\"unknown\","
        "\"filters\":[],\"grouping\":[],\"relations\":[],\"ordering_limit\":\"\"}},"
        "\"sql\":\"...\",\"summary_zh\":\"...\"}}）。\n"
        "仍须遵守：只读单语句；输出列严格按问句顺序且不增加未请求列；"
        "同一父实体跨多条子记录同时满足两个值时，不能用裸 IN 或同列互斥 AND 代替交集语义。\n"
        "数据库结构：\n{schema}\n\n"
        "用户问题：{question}\n"
        "错误的 SQL：{bad_sql}\n"
        "错误信息：{error}\n\n"
        "输出修正后的 JSON："
    )

    _CANDIDATE_REPAIR_PROMPT = (
        "你之前的 SQL 与本地独立关系代数合同存在可验证冲突。"
        "请在一次回答中给出 2–3 个有界修复候选，最多 3 个。\n"
        "修复焦点 JSON 是本地验收器的硬约束：所有候选必须先一致修复该冲突，"
        "不得把硬约束当作候选多样性。若是投影冲突，所有候选必须使用同一个"
        "精确最小输出集，不得重新加入已拒绝列。\n"
        "第一个候选是你的主修复；硬约束满足后，其余候选才在与问题有关的"
        "关系算子上有实质差异，例如 JOIN 路径、聚合层级、EXISTS/"
        "条件聚合、相关子查询或 tie 策略；不要只更换别名、大小写或格式。\n"
        "每个候选都必须是只读单语句，不得猜测 schema 未声明的关系，"
        "并且逐项实现本地合同。候选顺序不是正确性评分，本地验收器会独立筛选。\n"
        "若修复焦点包含 candidate_protocol.mode=projection_locked_sql_tail，"
        "每个候选必须省略 sql 并只返回 sql_tail；sql_tail 从 FROM 开始，"
        "包含完整 JOIN/WHERE/GROUP/HAVING/ORDER/LIMIT，且每个锁定输出表恰好引用一次。"
        "本地编译器会从 bindings 确定性生成 SELECT，你不得自行返回 SELECT 列表。\n"
        "只输出一个 JSON 对象："
        "{{\"candidates\":[{{\"candidate_id\":\"primary\","
        "\"strategy\":\"简短描述关系算子差异\",\"intent\":{{"
        "\"outputs\":[],\"row_grain\":\"unknown\",\"filters\":[],"
        "\"grouping\":[],\"relations\":[],\"ordering_limit\":\"\"}},"
        "\"sql\":\"普通模式的完整 SQL\","
        "\"sql_tail\":\"投影锁定模式专用，从 FROM 开始\","
        "\"summary_zh\":\"...\"}}]}}。\n"
        "数据库结构与本地合同：\n{schema}\n\n"
        "用户问题：{question}\n"
        "已拒绝 SQL：{bad_sql}\n"
        "修复焦点 JSON：{repair_focus}\n\n"
        "输出候选 JSON："
    )

    _CONTRACT_REVIEW_PROMPT = (
        "你是数据库查询的语义合同审查器。候选 SQL 已通过只读安全校验并执行，但‘能执行’不代表"
        "‘回答正确’。请逐项核对用户要求的输出、每行粒度、筛选、关联、分组、排序和条数。\n"
        "只有发现会改变答案语义的缺陷时才 revise；不要为了别名、大小写、CTE/子查询风格或其他"
        "等价写法改写。不要假设未在 schema/业务字典/用户证据中声明的关系。\n"
        "返回一个 JSON 对象：\n"
        "{{\"decision\":\"accept|revise\",\"intent\":{{"
        "\"outputs\":[\"按顺序的输出语义\"],"
        "\"row_grain\":\"single_value|single_row|detail_rows|one_row_per_entity|"
        "one_row_per_group|top_k|set_of_entities|unknown\","
        "\"filters\":[\"筛选口径\"],\"grouping\":[\"分组口径\"],"
        "\"relations\":[\"关联口径\"],\"ordering_limit\":\"排序与条数\"}},"
        "\"sql\":\"accept 时原样返回；revise 时返回完整修正 SQL\","
        "\"summary_zh\":\"一句话中文摘要\",\"reason_code\":\"简短错误类别\"}}。\n"
        "只输出 JSON，不输出思维过程或 markdown。\n\n"
        "数据库结构与相关业务字典：\n{schema}\n\n"
        "用户问题：{question}\n\n"
        "候选 SQL：{sql}\n"
        "执行签名（不含结果数据）：{observation}\n\n"
        "输出 JSON："
    )

    def __init__(self, security: SQLSecurity, schema: SchemaSnapshot, llm_cfg: str = "default"):
        self.security = security
        self.schema = schema
        self.llm_cfg = llm_cfg
        self.last_generated_sql = ""
        # Diagnostic-only state for benchmark/error taxonomy. Rejected SQL is
        # not placed in DBAnswer and cannot be mistaken for an accepted query.
        self.last_candidate_sql = ""
        self.last_semantic_hint = ""
        self.semantic_repair_count = 0
        self.last_query_intent = QueryIntentContract()
        self.last_relational_contract = RelationalAlgebraContract()
        self.last_relational_plan: Optional[Any] = None
        self.last_candidate_search: Optional[dict] = None

    @staticmethod
    def _bounded_candidate_text(value: Any, limit: int) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]

    @classmethod
    def _repair_candidates_from_payload(cls, payload: Any) -> List[dict]:
        """Parse a bounded repair portfolio while retaining legacy one-SQL replies."""
        if not isinstance(payload, dict):
            return []
        raw_candidates = payload.get("candidates")
        legacy_single = not isinstance(raw_candidates, list)
        if not isinstance(raw_candidates, list):
            # Compatibility matters for existing providers and stored tests. A
            # legacy single repair is treated as the primary candidate and is
            # still subjected to every local gate below.
            raw_candidates = [payload]
        parsed: List[dict] = []
        for index, raw in enumerate(raw_candidates[:3]):
            item = raw if isinstance(raw, dict) else {}
            candidate_id = cls._bounded_candidate_text(
                item.get("candidate_id") or f"candidate_{index + 1}", 48,
            )
            candidate_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", candidate_id).strip("_")
            parsed.append({
                "candidate_id": candidate_id or f"candidate_{index + 1}",
                "position": index,
                "strategy": cls._bounded_candidate_text(item.get("strategy"), 160),
                "sql": str(item.get("sql") or "").strip(),
                "sql_tail": str(item.get("sql_tail") or "").strip(),
                "summary_zh": cls._bounded_candidate_text(item.get("summary_zh"), 500),
                "intent": QueryIntentContract.from_payload(item.get("intent")),
                "legacy_single": legacy_single,
            })
        return parsed

    def _projection_lock_bindings(
        self, conflict: QuerySemanticConflict,
    ) -> List[dict]:
        raw_bindings = conflict.constraints.get("required_output_bindings")
        expected_columns = conflict.constraints.get("required_output_columns")
        relational = conflict.constraints.get("relational_contract")
        if isinstance(relational, dict):
            raw_bindings = relational.get("output_bindings")
            expected_columns = relational.get("output_columns")
            if relational.get("output_layout"):
                # The projection-lock compiler owns physical columns only; it
                # must never drop a requested aggregate output from a mixed
                # layout.  Mixed layouts are handled by the native grouped
                # plan or by ordinary full-candidate validation.
                return []
        if not isinstance(raw_bindings, list) or not raw_bindings:
            return []
        expected = [str(item) for item in expected_columns] \
            if isinstance(expected_columns, list) else []
        bindings: List[dict] = []
        for raw in raw_bindings:
            if not isinstance(raw, dict):
                return []
            table_name = str(raw.get("table") or "")
            column_name = str(raw.get("column") or "")
            raw_candidates = raw.get("table_candidates")
            table_candidates = [str(item) for item in raw_candidates] \
                if isinstance(raw_candidates, list) else []
            if table_name:
                table = self.schema.tables.get(table_name)
                if table is None or not any(
                    column.name == column_name for column in table.columns
                ):
                    return []
                binding = {"table": table_name, "column": column_name}
            else:
                if len(table_candidates) < 2 or any(
                    table_candidate not in self.schema.tables
                    or not any(
                        column.name == column_name
                        for column in self.schema.tables[table_candidate].columns
                    )
                    for table_candidate in table_candidates
                ):
                    return []
                binding = {
                    "table": "",
                    "column": column_name,
                    "table_candidates": table_candidates,
                }
            if binding not in bindings:
                bindings.append(binding)
        # Never lock only a subset of an exact output contract. Partial local
        # compilation would silently drop semantics and recreate the original
        # failure mode under a more authoritative-looking name.
        if expected and [item["column"] for item in bindings] != expected:
            return []
        return bindings

    @staticmethod
    def _local_compiler_assessment_allows_progress(
        assessment: dict,
        conflict: QuerySemanticConflict,
        *,
        allow_intermediate: bool,
    ) -> bool:
        """Accept an intermediate only when it exposes a different conflict."""
        if assessment.get("status") == "eligible":
            return True
        if (
            not allow_intermediate
            or assessment.get("reason_code") != "semantic_contract"
            or not isinstance(assessment.get("conflict"), dict)
        ):
            return False
        remaining = assessment["conflict"]
        next_code = str(remaining.get("code") or "")
        next_message = str(remaining.get("message") or "")
        return bool(next_code and next_message) and (
            next_code, next_message
        ) != (conflict.code, conflict.message)

    def _try_local_projection_repair(
        self,
        *,
        question: str,
        bad_sql: str,
        conflict: QuerySemanticConflict,
        allowed_tables: Optional[List[str]],
        allow_intermediate: bool = False,
    ) -> Optional[dict]:
        """Repair a rejected projection while preserving the proven SQL tail.

        This is not a general SQL rewrite.  It runs only after the local
        projection checker has produced a complete physical binding, retains
        the original FROM/WHERE/GROUP/ORDER plan byte-for-byte, then repeats
        scope, relation, read-only and semantic validation.
        """
        relational = conflict.constraints.get("relational_contract")
        relational_projection_only = bool(
            conflict.code == "relational_algebra_contract"
            and isinstance(relational, dict)
            and not relational.get("output_layout")
            and conflict.message.startswith(
                "schema 已将问句输出完整绑定为 "
            )
        )
        if (
            conflict.code != "projection" and not relational_projection_only
        ) or re.match(
            r"\s*WITH\b", bad_sql, re.IGNORECASE,
        ) or re.search(r"\b(?:UNION|INTERSECT|EXCEPT)\b", bad_sql, re.IGNORECASE):
            return None
        bindings = self._projection_lock_bindings(conflict)
        if not bindings:
            return None
        tail_match = re.match(
            r"\s*SELECT\s+.+?\s+(?P<tail>FROM\b.*)$",
            bad_sql,
            re.IGNORECASE | re.DOTALL,
        )
        if tail_match is None:
            return None
        try:
            repaired_sql = self._compile_projection_locked_sql(
                bindings, tail_match.group("tail"),
            )
            if re.match(r"\s*SELECT\s+DISTINCT\b", bad_sql, re.IGNORECASE):
                repaired_sql = re.sub(
                    r"^SELECT\b", "SELECT DISTINCT", repaired_sql,
                    count=1, flags=re.IGNORECASE,
                )
        except NL2SQLError:
            return None
        candidate = {
            "candidate_id": "local_projection_compiler",
            "position": 0,
            "strategy": "preserve_original_relational_tail",
            "sql": repaired_sql,
            "summary_zh": "",
            "intent": QueryIntentContract(),
            "projection_locked": True,
            "locked_output_columns": [item["column"] for item in bindings],
        }
        assessment = self._assess_repair_candidate(
            question, candidate, allowed_tables,
        )
        if not self._local_compiler_assessment_allows_progress(
            assessment, conflict, allow_intermediate=allow_intermediate,
        ):
            return None
        diagnostic = {
            "tool": "bounded_candidate_search",
            "version": "1.1",
            "status": "local_projection_compiled",
            "requested_max": 0,
            "received_count": 0,
            "distinct_count": 1,
            "eligible_count": int(assessment["status"] == "eligible"),
            "selected_candidate_id": candidate["candidate_id"],
            "selection_basis": "complete_physical_projection_and_preserved_tail",
            "candidate_protocol": "local_projection_preserved_tail",
            "model_calls": 0,
            "contract_coverage": {
                "outputs": True,
                "target_tables": True,
                "relational_tail_preserved": True,
            },
            "assessments": [assessment],
        }
        self.last_candidate_search = diagnostic
        return {"selected": candidate, "diagnostic": diagnostic}

    def _try_local_deterministic_tie_repair(
        self,
        *,
        question: str,
        bad_sql: str,
        conflict: QuerySemanticConflict,
        allowed_tables: Optional[List[str]],
        allow_intermediate: bool = False,
    ) -> Optional[dict]:
        """Append only a proven stable key to a simple single-row ranking.

        The aggregate, filters, joins, projection and primary ordering remain
        byte-for-byte unchanged.  Self-joins, set operations, CTEs and queries
        without an explicit ``ORDER BY ... LIMIT 1`` stay fail-closed.
        """
        contract = self.last_relational_contract
        if (
            conflict.code != "relational_algebra_contract"
            or contract.tie_policy != "single_row"
            or not contract.tie_breaker_columns
            or "稳定二级排序" not in conflict.message
            or re.match(r"\s*WITH\b", bad_sql, re.IGNORECASE)
            or re.search(
                r"\b(?:UNION|INTERSECT|EXCEPT)\b", bad_sql, re.IGNORECASE,
            )
        ):
            return None
        code = _sql_code_only(bad_sql, mask_identifiers=False)
        order = re.search(
            r"\bORDER\s+BY\b(?P<body>.*?)(?=\bLIMIT\b)",
            code,
            re.IGNORECASE | re.DOTALL,
        )
        limit = re.search(r"\bLIMIT\s+1\b", code, re.IGNORECASE)
        if order is None or limit is None or order.end() > limit.start():
            return None

        canonical = {name.casefold(): name for name in self.schema.tables}
        source_pattern = re.compile(
            r"\b(?:FROM|JOIN)\s+(?:ONLY\s+)?"
            r"(?:\"(?P<double>[^\"]+)\"|`(?P<backtick>[^`]+)`|"
            r"\[(?P<bracket>[^\]]+)\]|(?P<plain>[A-Za-z_][\w$]*))"
            r"(?:\s+(?:AS\s+)?(?P<alias>(?!(?:CROSS|EXCEPT|FETCH|FULL|GROUP|"
            r"HAVING|INNER|INTERSECT|JOIN|LEFT|LIMIT|ON|ORDER|RIGHT|UNION|WHERE)\b)"
            r"[A-Za-z_][\w$]*))?",
            re.IGNORECASE,
        )
        qualifiers: Dict[str, List[str]] = {}
        for source in source_pattern.finditer(code):
            raw = next(
                source.group(name)
                for name in ("double", "backtick", "bracket", "plain")
                if source.group(name) is not None
            )
            table_name = canonical.get(raw.casefold())
            if not table_name:
                continue
            qualifier = source.group("alias") or raw
            if qualifier not in qualifiers.setdefault(table_name, []):
                qualifiers[table_name].append(qualifier)
        if any(len(values) > 1 for values in qualifiers.values()):
            return None

        q = SQLiteRelationalPlanRenderer.quote_identifier
        ordered_refs: List[str] = []
        for qualified in contract.tie_breaker_columns:
            if "." not in qualified:
                return None
            target_table, target_column = qualified.rsplit(".", 1)
            equivalents = [(target_table, target_column)]
            for table in self.schema.tables.values():
                for column in table.columns:
                    if (
                        str(column.fk_table or "").casefold()
                        == target_table.casefold()
                        and str(column.fk_column or "").casefold()
                        == target_column.casefold()
                    ):
                        equivalents.append((table.name, column.name))
            resolved = [
                (table_name, column_name, qualifiers[table_name][0])
                for table_name, column_name in equivalents
                if len(qualifiers.get(table_name, [])) == 1
            ]
            if not resolved:
                return None
            _table_name, column_name, qualifier = resolved[0]
            ref = f"{q(qualifier)}.{q(column_name)} ASC"
            if ref not in ordered_refs:
                ordered_refs.append(ref)
        if not ordered_refs:
            return None

        repaired_sql = (
            bad_sql[:limit.start()].rstrip()
            + ", " + ", ".join(ordered_refs) + " "
            + bad_sql[limit.start():].lstrip()
        )
        candidate = {
            "candidate_id": "local_deterministic_tie_compiler",
            "position": 0,
            "strategy": "append_proven_stable_order_key",
            "sql": repaired_sql,
            "summary_zh": "",
            "intent": QueryIntentContract(),
            "projection_locked": False,
        }
        assessment = self._assess_repair_candidate(
            question, candidate, allowed_tables,
        )
        if not self._local_compiler_assessment_allows_progress(
            assessment, conflict, allow_intermediate=allow_intermediate,
        ):
            return None
        diagnostic = {
            "tool": "bounded_candidate_search",
            "version": "1.1",
            "status": "local_deterministic_tie_compiled",
            "requested_max": 0,
            "received_count": 0,
            "distinct_count": 1,
            "eligible_count": int(assessment["status"] == "eligible"),
            "selected_candidate_id": candidate["candidate_id"],
            "selection_basis": "proven_entity_key_and_preserved_query",
            "candidate_protocol": "local_order_by_extension",
            "model_calls": 0,
            "contract_coverage": {
                "primary_order_preserved": True,
                "stable_key_proven": True,
                "query_body_preserved": True,
            },
            "assessments": [assessment],
        }
        self.last_candidate_search = diagnostic
        return {"selected": candidate, "diagnostic": diagnostic}

    def _try_local_distinct_tuple_repair(
        self,
        *,
        question: str,
        bad_sql: str,
        conflict: QuerySemanticConflict,
        allowed_tables: Optional[List[str]],
        allow_intermediate: bool = False,
    ) -> Optional[dict]:
        """Add DISTINCT only when the contract proves a unique visible tuple.

        This compiler owns neither joins nor predicates.  It is deliberately
        limited to a simple, non-aggregate SELECT whose complete projection is
        already the exact tuple named by every distinct-row requirement.  The
        resulting candidate still passes the full scope, relation, read-only
        and semantic assessment, so an unrelated ALL_VALUES or filter defect
        cannot be hidden by adding DISTINCT.
        """
        contract = self.last_relational_contract
        code = _sql_code_only(bad_sql, mask_identifiers=False)
        if (
            conflict.code != "relational_algebra_contract"
            or not contract.distinct_row_requirements
            or "SELECT DISTINCT" not in conflict.message
            or re.match(r"\s*WITH\b", code, re.IGNORECASE)
            or re.search(
                r"\b(?:UNION|INTERSECT|EXCEPT|GROUP\s+BY|HAVING|"
                r"LIMIT|OFFSET|FETCH|TOP)\b",
                code,
                re.IGNORECASE,
            )
            or re.search(
                r"\b(?:COUNT|SUM|AVG|MIN|MAX|RANK|DENSE_RANK|ROW_NUMBER)\s*\(",
                code,
                re.IGNORECASE,
            )
            or re.match(r"\s*SELECT\s+ALL\b", code, re.IGNORECASE)
            or re.match(r"\s*SELECT\s+(?:ALL\s+)?DISTINCT\b", code, re.IGNORECASE)
        ):
            return None
        projected = self._simple_projection_columns(bad_sql)
        projection_match = re.match(
            r"\s*SELECT\s+(.*?)\s+FROM\b",
            bad_sql,
            re.IGNORECASE | re.DOTALL,
        )
        projection_items = (
            self._split_projection(projection_match.group(1))
            if projection_match is not None else []
        )
        required_tuples = [
            [str(value).rsplit(".", 1)[-1].casefold() for value in requirement.get("columns") or []]
            for requirement in contract.distinct_row_requirements
        ]
        if (
            not projected
            or len(projection_items) != len(projected)
            or any(not required or projected != required for required in required_tuples)
        ):
            return None
        selected = re.match(r"\s*SELECT\b", code, re.IGNORECASE)
        if selected is None:
            return None
        repaired_sql = (
            bad_sql[:selected.end()] + " DISTINCT" + bad_sql[selected.end():]
        )
        candidate = {
            "candidate_id": "local_distinct_tuple_compiler",
            "position": 0,
            "strategy": "add_proven_visible_tuple_distinctness",
            "sql": repaired_sql,
            "summary_zh": "",
            "intent": QueryIntentContract(),
            "projection_locked": False,
        }
        assessment = self._assess_repair_candidate(
            question, candidate, allowed_tables,
        )
        if not self._local_compiler_assessment_allows_progress(
            assessment, conflict, allow_intermediate=allow_intermediate,
        ):
            return None
        diagnostic = {
            "tool": "bounded_candidate_search",
            "version": "1.1",
            "status": "local_distinct_tuple_compiled",
            "requested_max": 0,
            "received_count": 0,
            "distinct_count": 1,
            "eligible_count": int(assessment["status"] == "eligible"),
            "selected_candidate_id": candidate["candidate_id"],
            "selection_basis": "complete_visible_tuple_contract",
            "candidate_protocol": "local_select_distinct_extension",
            "model_calls": 0,
            "contract_coverage": {
                "visible_tuple_complete": True,
                "query_body_preserved": True,
                "full_semantic_reassessment": True,
            },
            "assessments": [assessment],
        }
        self.last_candidate_search = diagnostic
        return {"selected": candidate, "diagnostic": diagnostic}

    def _try_local_exact_enum_repair(
        self,
        *,
        question: str,
        bad_sql: str,
        conflict: QuerySemanticConflict,
        allowed_tables: Optional[List[str]],
        allow_intermediate: bool = False,
    ) -> Optional[dict]:
        """Compile a uniquely sampled enum value without rewriting the query.

        The compiler changes only literal spelling, or a LIKE operator that
        the existing semantic gate has already proved to be an unauthorized
        broadening of one exact sampled value.  Projection, sources, joins,
        predicate columns/boolean structure and query shape stay untouched,
        and the candidate must pass the complete repair assessment before it
        can execute.
        """
        if conflict.code not in {
            "enum_literal_case", "wildcard_literal_broadening",
        }:
            return None
        expected_hint = (
            self._enum_literal_case_retry_hint(bad_sql)
            if conflict.code == "enum_literal_case"
            else self._wildcard_literal_retry_hint(question, bad_sql)
        )
        if not expected_hint or conflict.message != expected_hint:
            return None

        code = _sql_code_only(
            bad_sql, mask_identifiers=False, mask_literals=False,
        )
        tables_by_folded = {name.casefold(): name for name in self.schema.tables}
        columns_by_table = {
            table_name: {column.name.casefold(): column for column in table.columns}
            for table_name, table in self.schema.tables.items()
        }
        aliases: Dict[str, str] = {}
        for source_match in re.finditer(
            r"\b(?:FROM|JOIN)\s+"
            r"(?:\"([^\"]+)\"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))"
            r"(?:\s+(?:AS\s+)?(?!ON\b|WHERE\b|JOIN\b|GROUP\b|ORDER\b|LIMIT\b)"
            r"([A-Za-z_][\w$]*))?",
            code,
            re.IGNORECASE,
        ):
            raw_table = next(
                value for value in source_match.groups()[:4] if value is not None
            )
            physical = tables_by_folded.get(raw_table.casefold())
            if physical:
                aliases[raw_table.casefold()] = physical
                if source_match.group(5):
                    aliases[source_match.group(5).casefold()] = physical
        referenced_tables = self._sql_referenced_tables(bad_sql)

        def column_for(
            qualifier: Optional[str], raw_name: str,
        ) -> Optional[DBColumn]:
            folded = raw_name.strip('"`[]').casefold()
            if qualifier:
                table_name = aliases.get(qualifier.casefold())
                return columns_by_table.get(table_name, {}).get(folded) \
                    if table_name else None
            matches = [
                columns_by_table[table_name][folded]
                for table_name in referenced_tables
                if folded in columns_by_table.get(table_name, {})
            ]
            return matches[0] if len(matches) == 1 else None

        def canonical_sample(
            column: DBColumn, literal: str, *, require_spelling_change: bool,
        ) -> Optional[str]:
            matches = {
                str(value)
                for value in column.sample_values
                if value is not None
                and (
                    not require_spelling_change
                    or str(value) != literal
                )
                and str(value).casefold() == literal.casefold()
            }
            return next(iter(matches)) if len(matches) == 1 else None

        reference = (
            r"(?:(?P<qualifier>[A-Za-z_][\w$]*)\s*\.\s*)?"
            r"(?P<column>\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*)"
        )
        edits: List[tuple[int, int, str]] = []
        proof_columns: List[str] = []
        if conflict.code == "enum_literal_case":
            equality = re.compile(
                reference + r"\s*=\s*'(?P<literal>(?:''|[^'])*)'",
                re.IGNORECASE,
            )
            for match in equality.finditer(code):
                literal = match.group("literal").replace("''", "'")
                if not literal:
                    continue
                column = column_for(
                    match.group("qualifier"), match.group("column"),
                )
                canonical = canonical_sample(
                    column, literal, require_spelling_change=True,
                ) if column else None
                if canonical is None:
                    continue
                edits.append((
                    match.start("literal"), match.end("literal"),
                    canonical.replace("'", "''"),
                ))
                proof_columns.append(column.name)
        else:
            wildcard = re.compile(
                reference
                + r"(?P<operator>\s+LIKE\s+)"
                + r"(?P<quote>['\"])(?P<literal>.*?)(?P=quote)",
                re.IGNORECASE | re.DOTALL,
            )
            question_tokens = self._normalized_language_tokens(question)
            for match in wildcard.finditer(code):
                literal = match.group("literal")
                if not re.search(r"[%_]", literal):
                    continue
                core = re.sub(r"[%_]", " ", literal)
                core = re.sub(r"\s+", " ", core).strip()
                core_tokens = self._normalized_language_tokens(core)
                if not core_tokens or not core_tokens.issubset(question_tokens):
                    continue
                column = column_for(
                    match.group("qualifier"), match.group("column"),
                )
                canonical = canonical_sample(
                    column, core, require_spelling_change=False,
                ) if column else None
                if canonical is None:
                    continue
                edits.append((
                    match.start("operator"), match.end(),
                    " = '" + canonical.replace("'", "''") + "'",
                ))
                proof_columns.append(column.name)
        if not edits or len(edits) > 12:
            return None
        ordered_edits = sorted(edits)
        if any(
            previous[1] > current[0]
            for previous, current in zip(ordered_edits, ordered_edits[1:])
        ):
            return None
        repaired_sql = bad_sql
        for start, end, replacement in reversed(ordered_edits):
            repaired_sql = repaired_sql[:start] + replacement + repaired_sql[end:]

        candidate = {
            "candidate_id": "local_exact_enum_compiler",
            "position": 0,
            "strategy": "substitute_unique_sampled_enum_value",
            "sql": repaired_sql,
            "summary_zh": "",
            "intent": QueryIntentContract(),
            "projection_locked": False,
        }
        assessment = self._assess_repair_candidate(
            question, candidate, allowed_tables,
        )
        if not self._local_compiler_assessment_allows_progress(
            assessment, conflict, allow_intermediate=allow_intermediate,
        ):
            return None
        diagnostic = {
            "tool": "bounded_candidate_search",
            "version": "1.1",
            "status": "local_exact_enum_compiled",
            "requested_max": 0,
            "received_count": 0,
            "distinct_count": 1,
            "eligible_count": int(assessment["status"] == "eligible"),
            "selected_candidate_id": candidate["candidate_id"],
            "selection_basis": "unique_casefolded_schema_sample",
            "candidate_protocol": "local_exact_enum_substitution",
            "model_calls": 0,
            "contract_coverage": {
                "unique_sample_value": True,
                "query_structure_preserved": True,
                "full_semantic_reassessment": True,
                "columns": list(dict.fromkeys(proof_columns)),
            },
            "assessments": [assessment],
        }
        self.last_candidate_search = diagnostic
        return {"selected": candidate, "diagnostic": diagnostic}

    def _try_local_contract_repair(
        self,
        *,
        question: str,
        bad_sql: str,
        conflict: QuerySemanticConflict,
        allowed_tables: Optional[List[str]],
    ) -> Optional[dict]:
        """Compose monotonic local compilers, then require one final full pass."""
        compilers = (
            self._try_local_deterministic_tie_repair,
            self._try_local_projection_repair,
            self._try_local_distinct_tuple_repair,
            self._try_local_exact_enum_repair,
        )
        previous_diagnostic = self.last_candidate_search
        current_sql = bad_sql
        current_conflict = conflict
        seen_sql = {self._candidate_sql_key(bad_sql)}
        stages: List[dict] = []
        selected: Optional[dict] = None
        for _step in range(4):
            repair = None
            for compiler in compilers:
                repair = compiler(
                    question=question,
                    bad_sql=current_sql,
                    conflict=current_conflict,
                    allowed_tables=allowed_tables,
                    allow_intermediate=True,
                )
                if repair is not None:
                    break
            if repair is None or repair.get("selected") is None:
                self.last_candidate_search = previous_diagnostic
                return None
            selected = repair["selected"]
            diagnostic = repair.get("diagnostic") or {}
            stages.append(diagnostic)
            current_sql = str(selected.get("sql") or "").strip()
            sql_key = self._candidate_sql_key(current_sql)
            if not sql_key or sql_key in seen_sql:
                self.last_candidate_search = previous_diagnostic
                return None
            seen_sql.add(sql_key)
            assessment = (diagnostic.get("assessments") or [{}])[-1]
            if assessment.get("status") == "eligible":
                if len(stages) == 1:
                    return repair
                selected = {
                    **selected,
                    "candidate_id": "local_contract_pipeline_compiler",
                    "strategy": "compose_monotonic_local_contract_repairs",
                }
                pipeline_diagnostic = {
                    "tool": "bounded_candidate_search",
                    "version": "1.2",
                    "status": "local_contract_pipeline_compiled",
                    "requested_max": 0,
                    "received_count": 0,
                    "distinct_count": len(seen_sql) - 1,
                    "eligible_count": 1,
                    "selected_candidate_id": selected["candidate_id"],
                    "selection_basis": "monotonic_conflict_elimination",
                    "candidate_protocol": "local_contract_compiler_pipeline",
                    "model_calls": 0,
                    "pipeline_stages": [
                        str(stage.get("status") or "") for stage in stages
                    ],
                    "contract_coverage": {
                        "bounded_steps": True,
                        "no_sql_cycles": True,
                        "monotonic_conflicts": True,
                        "full_semantic_reassessment": True,
                    },
                    "assessments": [
                        item
                        for stage in stages
                        for item in (stage.get("assessments") or [])
                    ],
                }
                self.last_candidate_search = pipeline_diagnostic
                return {
                    "selected": selected,
                    "diagnostic": pipeline_diagnostic,
                }
            next_conflict = self._semantic_conflict(question, current_sql)
            if next_conflict is None or (
                next_conflict.code, next_conflict.message
            ) == (current_conflict.code, current_conflict.message):
                self.last_candidate_search = previous_diagnostic
                return None
            current_conflict = next_conflict
        self.last_candidate_search = previous_diagnostic
        return None

    def _candidate_search_readiness(
        self, question: str, conflict: QuerySemanticConflict,
    ) -> dict:
        """Prove enough independent dimensions before paying for candidates."""
        contract = self.last_relational_contract
        outputs_complete = bool(
            contract.output_columns
            and len(contract.output_bindings) == len(contract.output_columns)
            and [item.get("column") for item in contract.output_bindings]
            == contract.output_columns
            and all(item.get("table") for item in contract.output_bindings)
        )
        if contract.output_layout:
            outputs_complete = bool(
                outputs_complete
                and len(contract.output_layout) == len(contract.output_bindings) + 1
                and contract.output_layout[-1].get("kind") == "aggregate"
            )
        table_names = {
            str(item.get("table") or "") for item in contract.output_bindings
            if item.get("table")
        }
        for path in contract.relation_paths:
            table_names.update(str(item) for item in path.get("tables") or [])
        targets_complete = bool(table_names)
        asks_superlative = bool(re.search(
            r"\b(?:most|least|highest|lowest|top\s+1|fewest)\b|"
            r"最多|最少|最高|最低",
            question,
            re.IGNORECASE,
        ))
        cardinality_complete = not asks_superlative or contract.tie_policy in {
            "single_row", "all_ties",
        }
        asks_aggregate = bool(re.search(
            r"\b(?:count|number\s+of|average|sum|total|most|least|fewest)\b|"
            r"数量|平均|合计|最多|最少",
            question,
            re.IGNORECASE,
        ))
        aggregation_complete = not asks_aggregate or bool(
            contract.aggregation_stages or contract.aggregate_requirements
            or contract.distinct_count_requirements or contract.ordering_requirements
        )
        asks_set = bool(re.search(
            r"\bboth\b.+\band\b|\b(?:intersect|intersection)\b|同时|交集",
            question,
            re.IGNORECASE | re.DOTALL,
        ))
        set_complete = not asks_set or bool(contract.set_requirements)
        asks_filter = bool(re.search(
            r"\b(?:where|with|having|after|before|less\s+than|more\s+than|"
            r"at\s+least|at\s+most|exactly)\b|其中|条件|过滤|大于|小于",
            question,
            re.IGNORECASE,
        ))
        filters_complete = not asks_filter or bool(
            contract.filter_requirements or contract.modifier_filters
            or contract.boolean_filter_requirements
            or contract.relationship_thresholds or contract.correlation_requirements
        )
        coverage = {
            "outputs": outputs_complete,
            "target_tables": targets_complete,
            "row_cardinality": cardinality_complete,
            "filters": filters_complete,
            "set_semantics": set_complete,
            "aggregation": aggregation_complete,
        }
        missing = [name for name, complete in coverage.items() if not complete]
        return {
            "ready": not missing and conflict.code == "relational_algebra_contract",
            "coverage": coverage,
            "missing_dimensions": missing,
        }

    def _compile_projection_locked_sql(self, bindings: List[dict], sql_tail: str) -> str:
        """Compile a SELECT list owned by the local contract over a model plan tail."""
        tail = str(sql_tail or "").strip()
        if not re.match(r"^FROM\b", tail, re.IGNORECASE):
            raise NL2SQLError("projection-locked 候选必须从 FROM 开始")
        if re.match(r"^FROM\s*\(", tail, re.IGNORECASE):
            raise NL2SQLError("projection-locked 候选不支持派生表作为首数据源")
        source_pattern = re.compile(
            r"\b(?:FROM|JOIN)\s+(?P<table>[A-Za-z_][\w$]*)"
            r"(?:\s+(?:AS\s+)?(?P<alias>(?!ON\b|WHERE\b|JOIN\b|GROUP\b|"
            r"ORDER\b|HAVING\b|LIMIT\b)[A-Za-z_][\w$]*))?",
            re.IGNORECASE,
        )
        schema_names = {name.casefold(): name for name in self.schema.tables}
        aliases_by_table: Dict[str, List[str]] = {}
        for source in source_pattern.finditer(tail):
            physical = schema_names.get(source.group("table").casefold())
            if not physical:
                continue
            qualifier = source.group("alias") or source.group("table")
            if qualifier not in aliases_by_table.setdefault(physical, []):
                aliases_by_table[physical].append(qualifier)
        projection: List[str] = []
        for binding in bindings:
            binding_table = binding["table"]
            if not binding_table:
                referenced_candidates = [
                    table_name for table_name in binding.get("table_candidates") or []
                    if aliases_by_table.get(table_name)
                ]
                if len(referenced_candidates) != 1:
                    raise NL2SQLError(
                        "projection-locked 候选无法从关系计划唯一解析输出列所属表: "
                        + binding["column"]
                    )
                binding_table = referenced_candidates[0]
            qualifiers = aliases_by_table.get(binding_table, [])
            if len(qualifiers) != 1:
                raise NL2SQLError(
                    "projection-locked 候选必须唯一引用输出列所属表: "
                    + binding_table
                )
            qualifier = qualifiers[0]
            column_name = binding["column"].replace('"', '""')
            projection.append(f'{qualifier}."{column_name}"')
        if not projection:
            raise NL2SQLError("projection-locked 候选缺少完整输出绑定")
        return "SELECT " + ", ".join(projection) + " " + tail

    @staticmethod
    def _candidate_sql_key(sql: str) -> str:
        """Conservatively deduplicate byte-equivalent SQL modulo whitespace."""
        statement = str(sql or "").strip()
        if statement.endswith(";"):
            statement = statement[:-1].rstrip()
        # Deliberately retain identifier and literal case: case can change
        # semantics in sampled enums, so a broader canonicalizer would merge
        # candidates that the local contract must assess independently.
        return re.sub(r"\s+", " ", statement)

    def _assess_repair_candidate(
        self,
        question: str,
        candidate: dict,
        allowed_tables: Optional[List[str]],
    ) -> dict:
        sql = str(candidate.get("sql") or "").strip()
        base = {
            "candidate_id": candidate["candidate_id"],
            "position": int(candidate["position"]),
            "strategy": candidate["strategy"],
            "projection_locked": bool(candidate.get("projection_locked")),
            "locked_output_columns": list(candidate.get("locked_output_columns") or []),
            "observed_projection": self._simple_projection_columns(sql)[:12] if sql else [],
            "referenced_tables": self._sql_referenced_tables(sql)[:12] if sql else [],
        }
        if candidate.get("compile_error"):
            return {
                **base, "status": "rejected", "reason_code": "candidate_plan_compile",
                "detail": self._bounded_candidate_text(candidate["compile_error"], 500),
            }
        if not sql:
            return {**base, "status": "rejected", "reason_code": "missing_sql", "detail": "LLM 未返回 SQL"}
        if allowed_tables:
            try:
                self._validate_allowed_tables(sql, allowed_tables)
            except NL2SQLError as exc:
                return {
                    **base, "status": "rejected", "reason_code": "branch_scope",
                    "detail": self._bounded_candidate_text(exc, 500),
                }
        relation_block = self._relation_clarification(sql, question)
        if relation_block is not None:
            return {
                **base, "status": "rejected", "reason_code": "table_relationship",
                "detail": self._bounded_candidate_text(
                    (relation_block.clarification or {}).get("question"), 500,
                ),
            }
        try:
            self.security.validate(sql)
        except SQLSecurityError as exc:
            return {
                **base, "status": "rejected", "reason_code": "sql_security",
                "detail": self._bounded_candidate_text(exc, 500),
            }
        semantic_conflict = self._semantic_conflict(
            question,
            sql,
            locked_projection_columns=candidate.get("locked_output_columns"),
        )
        if semantic_conflict:
            return {
                **base, "status": "rejected", "reason_code": "semantic_contract",
                "detail": self._bounded_candidate_text(semantic_conflict.message, 500),
                "conflict": semantic_conflict.as_dict(),
            }
        return {**base, "status": "eligible", "reason_code": "", "detail": ""}

    def _search_semantic_repair_candidates(
        self,
        *,
        question: str,
        schema_txt: str,
        bad_sql: str,
        semantic_conflict: QuerySemanticConflict,
        history: Optional[list],
        allowed_tables: Optional[List[str]],
    ) -> dict:
        """Generate once, gate every candidate, and select only bounded evidence.

        The first portfolio item preserves the existing direct-repair path. If
        it still conflicts, a replacement is accepted only when exactly one
        distinct alternative survives the independent scope, relationship,
        read-only and semantic gates. Multiple distinct survivors are not
        ranked by model order or self-rating; they fail closed as ambiguous.
        """
        locked_bindings = self._projection_lock_bindings(semantic_conflict)
        repair_focus = semantic_conflict.as_dict()
        if locked_bindings:
            repair_focus["candidate_protocol"] = {
                "mode": "projection_locked_sql_tail",
                "bindings": [dict(item) for item in locked_bindings],
                "invariant": (
                    "Local compiler owns SELECT. Return only a FROM-starting sql_tail; "
                    "do not add, remove, merge, or reorder output columns."
                ),
            }
        prompt = self._CANDIDATE_REPAIR_PROMPT.format(
            schema=schema_txt,
            question=question,
            bad_sql=bad_sql,
            repair_focus=json.dumps(
                repair_focus,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        )
        payload = _llm_ask_json(prompt, self.llm_cfg, history=history)
        candidates = self._repair_candidates_from_payload(payload)
        if locked_bindings:
            for candidate in candidates:
                if candidate.get("legacy_single"):
                    # Existing providers that still return a single complete
                    # repair keep the validated compatibility path. Portfolio
                    # replies use the stricter plan/compiler protocol.
                    continue
                try:
                    candidate["sql"] = self._compile_projection_locked_sql(
                        locked_bindings, candidate.get("sql_tail") or "",
                    )
                    candidate["projection_locked"] = True
                    candidate["locked_output_columns"] = [
                        item["column"] for item in locked_bindings
                    ]
                except NL2SQLError as exc:
                    candidate["sql"] = ""
                    candidate["compile_error"] = str(exc)
        if candidates:
            self.last_candidate_sql = str(candidates[0]["sql"] or "")

        seen: set[str] = set()
        eligible: List[dict] = []
        assessments: List[dict] = []
        for candidate in candidates:
            key = self._candidate_sql_key(candidate["sql"])
            if key and key in seen:
                assessments.append({
                    "candidate_id": candidate["candidate_id"],
                    "position": int(candidate["position"]),
                    "strategy": candidate["strategy"],
                    "status": "rejected",
                    "reason_code": "duplicate_sql",
                    "detail": "与先前候选重复",
                })
                continue
            if key:
                seen.add(key)
            assessment = self._assess_repair_candidate(
                question, candidate, allowed_tables,
            )
            assessments.append(assessment)
            if assessment["status"] == "eligible":
                eligible.append(candidate)

        primary = candidates[0] if candidates else None
        primary_eligible = bool(
            primary and any(
                item["position"] == 0 and item["status"] == "eligible"
                for item in assessments
            )
        )
        selected: Optional[dict] = None
        if primary_eligible:
            selected = primary
            status = "primary_accepted"
        else:
            alternatives = [item for item in eligible if int(item["position"]) > 0]
            if len(alternatives) == 1:
                selected = alternatives[0]
                status = "unique_alternative_accepted"
            elif len(alternatives) > 1:
                status = "ambiguous_alternatives"
            else:
                status = "no_eligible_candidate"

        diagnostic = {
            "tool": "bounded_candidate_search",
            "version": "1.0",
            "status": status,
            "requested_max": 3,
            "received_count": len(candidates),
            "distinct_count": len(seen),
            "eligible_count": len(eligible),
            "selected_candidate_id": (
                selected["candidate_id"] if selected is not None else None
            ),
            "selection_basis": (
                "validated_primary" if status == "primary_accepted"
                else "unique_independently_validated_alternative"
                if status == "unique_alternative_accepted"
                else "fail_closed_without_independent_tie_breaker"
            ),
            "candidate_protocol": (
                "projection_locked_sql_tail" if locked_bindings else "complete_sql"
            ),
            "assessments": assessments,
        }
        self.last_candidate_search = diagnostic
        return {"selected": selected, "diagnostic": diagnostic}

    @staticmethod
    def _requires_contract_review(
        question: str,
        sql: str,
        allowed_tables: Optional[List[str]],
        result: Optional[SQLResult] = None,
    ) -> bool:
        """Run the expensive reviewer only for suspicious empty complex results.

        A completed read-only statement is not automatically correct, but the
        BIRD ablation showed that reviewing every complex success added extreme
        latency and introduced new semantic regressions.  Empty complex results
        are a locally observable failure signal; non-empty candidates rely on
        deterministic contracts and remain single-call.
        """
        if allowed_tables or result is None or bool(result.rows):
            return False
        code = _sql_code_only(sql, mask_identifiers=False)
        if re.search(
            r"\b(?:JOIN|WITH|GROUP\s+BY|HAVING|UNION|INTERSECT|EXCEPT|CASE|OVER)\b|"
            r"\(\s*SELECT\b",
            code,
            re.IGNORECASE,
        ):
            return True
        complex_question = re.search(
            r"\b(?:each|every|both|all|percentage|percent|proportion|ratio|difference|"
            r"rank|top|most|least|highest|lowest|average|total|more\s+than|less\s+than|"
            r"before|after|between)\b|分别|各自|每个|同时|比例|百分比|差值|排名|最高|最低|"
            r"最多|最少|平均|总计|超过|少于|介于",
            question,
            re.IGNORECASE,
        )
        return bool(complex_question and re.search(
            r"\b(?:COUNT|SUM|AVG|MIN|MAX|DISTINCT|ORDER\s+BY|LIMIT)\b",
            code,
            re.IGNORECASE,
        ))

    def _review_candidate(
        self,
        question: str,
        schema_txt: str,
        sql: str,
        result: SQLResult,
    ) -> dict:
        observation = json.dumps({
            "columns": list(result.columns),
            "row_count": int(result.row_count),
            "truncated": bool(result.truncated),
            "empty": not bool(result.rows),
        }, ensure_ascii=False, separators=(",", ":"))
        prompt = self._CONTRACT_REVIEW_PROMPT.format(
            schema=schema_txt,
            question=question,
            sql=sql,
            observation=observation,
        )
        try:
            payload = _llm_ask_json(prompt, self.llm_cfg)
        except DBAgentError as exc:
            return {
                "status": "unavailable",
                "decision": "accept",
                "reason_code": type(exc).__name__,
                "contract": QueryIntentContract(),
                "sql": sql,
                "summary_zh": "",
            }
        contract = QueryIntentContract.from_payload(payload.get("intent"))
        decision = str(payload.get("decision") or "").strip().lower()
        revised_sql = str(payload.get("sql") or "").strip()
        if decision != "revise" or not revised_sql:
            decision = "accept"
            revised_sql = sql
        return {
            "status": "reviewed",
            "decision": decision,
            "reason_code": re.sub(
                r"\s+", " ", str(payload.get("reason_code") or ""),
            ).strip()[:120],
            "contract": contract,
            "sql": revised_sql,
            "summary_zh": re.sub(
                r"\s+", " ", str(payload.get("summary_zh") or ""),
            ).strip()[:500],
        }

    def _schema_context(
        self,
        question: str = "",
        allowed_tables: Optional[List[str]] = None,
    ) -> str:
        if not allowed_tables:
            base = self.schema.compact_for_question(question)
        else:
            allowed = set(allowed_tables)
            scoped = SchemaSnapshot(
                db_path=self.schema.db_path,
                tables={name: table for name, table in self.schema.tables.items() if name in allowed},
                generated_at=self.schema.generated_at,
            )
            base = scoped.compact_for_question(question)
        extras = self._grounded_schema_hints(question, allowed_tables)
        extras.extend(self._schema_semantic_hints(allowed_tables))
        if extras:
            return base + "\n\n" + "\n".join(extras)
        return base

    def _grounded_schema_hints(
        self,
        question: str,
        allowed_tables: Optional[List[str]] = None,
    ) -> List[str]:
        """Expose only question-matched values and non-sensitive storage shapes.

        SchemaDiscovery samples are intentionally *not* dumped wholesale into an
        external model prompt.  When the user already supplied a value, however,
        a case-insensitive sample match can safely return the canonical database
        spelling.  Date/time samples contribute only a format shape, never an
        unrelated stored value.  This turns value grounding into a bounded,
        reusable pre-generation stage instead of a growing list of SQL regexes.
        """
        question_text = str(question or "")
        question_folded = question_text.casefold()
        allowed = set(allowed_tables) if allowed_tables is not None else None
        exact: List[str] = []
        shapes: List[str] = []
        date_question = bool(re.search(
            r"\b(?:date|time|day|week|month|quarter|year|january|february|march|"
            r"april|may|june|july|august|september|october|november|december)\b|"
            r"日期|时间|当天|当日|年|月|日|\b\d{4}[-/]\d{1,2}(?:[-/]\d{1,2})?\b",
            question_text,
            re.IGNORECASE,
        ))
        for table_name, table in self.schema.tables.items():
            if allowed is not None and table_name not in allowed:
                continue
            for column in table.columns:
                for raw in column.sample_values:
                    value = str("" if raw is None else raw).strip()
                    if not value or len(value) > 80:
                        continue
                    folded = value.casefold()
                    if len(folded) >= 2 and folded in question_folded:
                        hint = (
                            f"值绑定：用户已提供的值在 {table_name}.{column.name} 中的"
                            f"真实拼写为 {value!r}。等值筛选应使用该精确拼写。"
                        )
                        if hint not in exact:
                            exact.append(hint)
                    if not date_question or not is_time_column(column):
                        continue
                    shape = ""
                    if re.fullmatch(r"\d{4}-\d{2}-\d{2}T.+", value):
                        shape = "YYYY-MM-DDTHH:MM:SS"
                    elif re.fullmatch(r"\d{4}-\d{2}-\d{2} .+", value):
                        shape = "YYYY-MM-DD HH:MM:SS"
                    elif re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                        shape = "YYYY-MM-DD"
                    elif re.fullmatch(r"\d{8}", value):
                        shape = "YYYYMMDD"
                    elif re.fullmatch(r"\d{6}", value):
                        shape = "YYYYMM"
                    if shape:
                        hint = f"时间存储形状：{table_name}.{column.name} 使用 {shape}。"
                        if hint not in shapes:
                            shapes.append(hint)
        return (exact[:12] + shapes[:8])[:16]

    def _schema_semantic_hints(self, allowed_tables: Optional[List[str]] = None) -> List[str]:
        """给 NL2SQL 附加领域语义提示，消除模型对层级/枚举字段的猜测漂移。

        仅当 schema 中存在对应表/列时注入；提示是通用规则，不绑定具体关键词。
        """
        hints: List[str] = []
        tables = self.schema.tables
        pay_hint = self._payment_status_hint(allowed_tables)
        if pay_hint:
            hints.append(pay_hint)
        name_hint = self._person_name_hint(allowed_tables)
        if name_hint:
            hints.append(name_hint)
        topic_hint = self._knowledge_topic_hint(allowed_tables)
        if topic_hint:
            hints.append(topic_hint)
        role_hint = self._paired_role_fk_hint(allowed_tables)
        if role_hint:
            hints.append(role_hint)
        return hints

    _SOURCE_ROLE_TOKENS = {
        "source", "src", "origin", "from", "departure", "departing", "sender",
    }
    _TARGET_ROLE_TOKENS = {
        "dest", "destination", "target", "to", "arrival", "arriving", "receiver",
    }

    def _paired_role_fk_hint(self, allowed_tables: Optional[List[str]]) -> str:
        """提示同一事实表指向同一实体的 source/destination 双角色外键。

        这类 schema 常见于航班、转账、消息和迁移记录。问句没有限定角色时，
        任意只选一侧会漏数；提示只暴露已声明的真实 FK，不自动改写 SQL。
        """
        allowed = set(allowed_tables) if allowed_tables is not None else None
        for table_name, table in self.schema.tables.items():
            if allowed is not None and table_name not in allowed:
                continue
            by_target: Dict[tuple[str, str], List[DBColumn]] = {}
            for column in table.columns:
                if not column.fk_table or not column.fk_column:
                    continue
                if allowed is not None and column.fk_table not in allowed:
                    continue
                by_target.setdefault((column.fk_table, column.fk_column), []).append(column)
            for (target_table, target_column), columns in by_target.items():
                sources = [
                    column for column in columns
                    if self.schema._identifier_tokens(column.name) & self._SOURCE_ROLE_TOKENS
                ]
                targets = [
                    column for column in columns
                    if self.schema._identifier_tokens(column.name) & self._TARGET_ROLE_TOKENS
                ]
                if not sources or not targets:
                    continue
                source_names = ", ".join(f"{table_name}.{column.name}" for column in sources)
                target_names = ", ".join(f"{table_name}.{column.name}" for column in targets)
                return (
                    f"多角色外键提示：{source_names} 与 {target_names} 都指向 "
                    f"{target_table}.{target_column}，表示同一实体的不同角色。询问该实体未限定角色的"
                    "总体关联数量时，不要任意只统计其中一侧；应按问题合并相关角色。只有问题明确"
                    "限定来源/出发或目标/到达时，才只使用对应外键。合并总体次数时，应直接合并"
                    "各角色的原始关联记录后再 COUNT，或对各角色已聚合的计数做 SUM；不能对两个"
                    "已聚合分支再用外层 COUNT(*)，那只会数角色分支而不是关联记录。"
                )
        return ""

    @staticmethod
    def _normalized_language_tokens(value: str) -> set[str]:
        """为输出列复核提供轻量英文单复数归一，不承担完整语义解析。"""
        tokens = SchemaSnapshot._identifier_tokens(value)
        normalized: set[str] = set()
        for token in tokens:
            without_numeric_suffix = re.sub(r"\d+$", "", token)
            if without_numeric_suffix != token and len(without_numeric_suffix) >= 2:
                normalized.add(without_numeric_suffix)
            if not re.fullmatch(r"[a-z]+", token):
                normalized.add(token)
                continue
            if len(token) > 4 and token.endswith("ies"):
                normalized.add(token[:-3] + "y")
            elif (
                len(token) > 2 and token.endswith("s")
                and not token.endswith(("ss", "us", "is"))
            ):
                normalized.add(token[:-1])
            else:
                normalized.add(token)
        if "page" in normalized:
            normalized.update({"url", "website", "link"})
        # Common analytical wording can name the same physical concept with a
        # different part of speech.  Keep this deliberately narrow: it only
        # bridges the conventional ``LifeExpectancy`` column to phrases such
        # as "expected life length" and does not invent a general thesaurus.
        if "life" in normalized and normalized & {"expected", "expectancy"}:
            normalized.add("expectancy")
        # Narrow output-language aliases.  These are bidirectional only for
        # conventional schema concepts and are still resolved against a real,
        # unique physical column before they become an executable binding.
        if normalized & {"country", "nation", "nationality"}:
            normalized.update({"country", "nation", "nationality"})
        if "adress" in normalized:
            normalized.add("address")
        if "old" in normalized or "age" in normalized:
            normalized.update({"age", "old"})
        irregular = {
            "children": "child", "people": "person", "men": "man",
            "women": "woman",
        }
        for plural, singular in irregular.items():
            if plural in normalized:
                normalized.discard(plural)
                normalized.add(singular)
        return normalized

    @staticmethod
    def _question_requests_fuzzy_matching(value: str) -> bool:
        """Recognize explicit fuzzy intent without treating ``matches`` as a noun.

        Database questions commonly mention match/matches tables or records.
        A bare occurrence therefore cannot authorize wildcard broadening.
        """
        return bool(re.search(
            r"\b(?:contain|contains|containing|include|includes|including|"
            r"starts?\s+with|ends?\s+with|substring|partial|pattern|"
            r"similar\s+to)\b|"
            r"\b(?:fuzzy|partial|pattern|substring)\s+match(?:es|ing)?\b|"
            r"\bmatch(?:es|ing)?\s+(?:the\s+)?"
            r"(?:pattern|substring|text|value|fragment)\b|"
            r"包含|含有|开头|结尾|模糊|子串|模式匹配",
            value,
            re.IGNORECASE,
        ))

    @staticmethod
    def _entity_name_match(
        column_tokens: set[str], metadata_tokens: set[str], output_tokens: set[str],
    ) -> bool:
        """Map generic entity outputs ("which user") to a declared name column."""
        entity_tokens = {
            "account", "author", "customer", "driver", "employee", "member",
            "patient", "person", "player", "professional", "school", "student",
            "teacher", "team", "user",
        }
        generic_tokens = {"a", "an", "full", "name", "the", "which", "who"}
        return bool(
            "name" in column_tokens
            and (metadata_tokens & output_tokens & entity_tokens)
            and not (output_tokens - entity_tokens - generic_tokens)
        )

    @staticmethod
    def _output_request_phrase(question: str) -> str:
        """保守截取问句中的输出短语，排除后续筛选、排序和关系条件。"""
        text, _, evidence = str(question or "").strip().partition(
            "Relevant business evidence supplied by the user:"
        )
        text = text.strip()

        # Keep the entity qualifier attached to generic output labels.  Bare
        # ``id``/``name`` tokens often exist on several tables, so trimming at
        # ``of`` before preserving ``museum id and name`` can bind a perfectly
        # clear request to an unrelated table.  This bounded leading-question
        # grammar does not rewrite arbitrary phrases such as "feature type
        # name of feature AirCon"; it only owns an explicit answer frame whose
        # output consists entirely of generic labels followed by one entity.
        qualified_output = re.match(
            r"^\s*(?:what\s+(?:is|are|was|were)|list|show|give(?:\s+me)?|"
            r"state|provide|return)\s+(?:the\s+)?"
            r"(?P<labels>(?:names?|ids?|identifiers?|codes?|descriptions?|"
            r"locations?|dates?)(?:\s*(?:,|and)\s*(?:the\s+)?"
            r"(?:names?|ids?|identifiers?|codes?|descriptions?|locations?|dates?))*)"
            r"\s+of\s+(?:the\s+)?"
            r"(?P<entity>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*){0,2}?)"
            r"(?=\s+\b(?:that|which|who|with|having|where|whose|visited|"
            r"belonging|recorded|produced|used)\b|[?.,]|$)",
            text,
            re.IGNORECASE,
        )

        # Preserve coordinated output nouns before the generic ``of`` boundary.
        # Example: "package options and the name of the series for ..." has two
        # outputs; treating every ``of`` phrase as a filter used to drop
        # ``series`` and falsely reject the correct projection.  Requiring a
        # preceding conjunction/comma avoids changing single reference phrases
        # such as "feature type name of feature AirCon".
        text = re.sub(
            r"(?P<prefix>\b(?:and|plus)\s+(?:the\s+)?|,\s*(?:the\s+)?)"
            r"(?P<label>names?|ids?|identifiers?|codes?|descriptions?|locations?|dates?)"
            r"\s+of\s+(?:the\s+)?"
            r"(?P<entity>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*){0,2}?)"
            r"(?=\s+\b(?:for|where|whose|that|they|with|having|who|by|on)\b|[?.,]|$)",
            lambda match: (
                f"{match.group('prefix')}{match.group('entity')} "
                f"{match.group('label')}"
            ),
            text,
            flags=re.IGNORECASE,
        )

        def trim(value: str) -> str:
            return re.split(
                r"\b(?:of|for(?:\s+the)?|where|whose|that|with|having|who|by|on|"
                r"corres(?:pond|ond)ing\s+to)\b|[?。]",
                value,
                maxsplit=1,
                flags=re.IGNORECASE,
            )[0].strip()

        trailing_output = re.search(
            r"\bshowing\s+(.+?)(?:[?.。]|$)", text, re.IGNORECASE,
        )
        scalar_how_many = re.search(
            r"\bhow\s+many\s+(?P<output>[A-Za-z][\w-]*"
            r"(?:\s+[A-Za-z][\w-]*){0,2}?)\s+"
            r"(?:does|do|did|has|have|had)\b",
            text,
            re.IGNORECASE,
        )
        contextual_what = re.search(
            r"\bwhat(?:\s+(?:is|are|was|were)|'s)\s+(.+)", text, re.IGNORECASE,
        )
        if qualified_output:
            phrase = (
                qualified_output.group("entity") + " "
                + qualified_output.group("labels")
            ).strip()
        elif scalar_how_many:
            # ``how many cylinders does the version have`` can ask for one
            # schema-bound scalar attribute rather than a row COUNT.  Returning
            # the bounded noun lets the physical-column resolver decide; if no
            # unique column exists, no executable projection is invented.
            phrase = scalar_how_many.group("output").strip()
        elif contextual_what:
            primary = trim(contextual_what.group(1))
            imperative = re.search(
                r"\b(?:list|show|give|state|provide|return)(?:\s+me)?\s+"
                r"(.+?)(?:[?.。]|$)",
                text,
                re.IGNORECASE,
            )
            phrase = " ".join(filter(None, [
                primary,
                trim(imperative.group(1)) if imperative else "",
                trim(trailing_output.group(1)) if trailing_output else "",
            ]))
        elif trailing_output:
            phrase = trim(trailing_output.group(1))
        else:
            imperative = re.search(
                r"\b(?:list|show|give|state|provide|return)(?:\s+me)?\s+(.+)",
                text,
                re.IGNORECASE,
            )
            if imperative:
                phrase = trim(imperative.group(1))
            else:
                which = re.match(
                    r"^which\s+(.+?)\s+(?:does|do|did|has|have|had|is|are)\b",
                    text,
                    re.IGNORECASE,
                )
                if which:
                    phrase = which.group(1).strip()
                else:
                    phrase = re.split(
                        r"\b(?:of|for\s+the|where|whose|that|with|having|who|"
                        r"corres(?:pond|ond)ing\s+to|by)\b",
                        text,
                        maxsplit=1,
                        flags=re.IGNORECASE,
                    )[0].strip()
        # BIRD-style expert evidence can explicitly bind an output phrase to a
        # tuple of physical columns.  Only parenthesized multi-column mappings
        # are used here; filter/value evidence remains outside output parsing.
        for mapping in re.finditer(
            r"([^;\r\n]+?)\s+refers\s+to\s+\(([^)]+)\)", evidence,
            re.IGNORECASE,
        ):
            left = mapping.group(1).strip(" '`")
            if left and left.casefold() in phrase.casefold():
                phrase = re.sub(
                    re.escape(left), mapping.group(2), phrase,
                    count=1, flags=re.IGNORECASE,
                )
        if re.search(
            r"\b(?:and\s+)?(?:also\s+)?how\s+old\s+(?:are|is|were|was)\s+"
            r"(?:they|he|she|it)\b",
            text,
            re.IGNORECASE,
        ):
            phrase += " age"
        # Preserve explicit follow-up outputs across sentence boundaries.
        # A second imperative such as "Please also give the full street
        # address" is part of the result tuple, not commentary or a filter.
        # Only an explicit ``also`` answer frame is merged, so unrelated later
        # imperatives do not silently widen the projection contract.
        for secondary in re.finditer(
            r"\b(?:please\s+)?also\s+"
            r"(?:list|show|give|state|provide|return)(?:\s+me)?\s+"
            r"(?P<output>.+?)(?:[?.。]|$)",
            text,
            re.IGNORECASE,
        ):
            extra = trim(secondary.group("output"))
            if extra and extra.casefold() not in phrase.casefold():
                phrase = f"{phrase} {extra}".strip()
        return phrase

    def _order_output_bindings_by_question(
        self, output_phrase: str, bindings: List[dict],
    ) -> List[dict]:
        """Order complete physical output bindings by their lexical request order.

        Schema discovery order is an implementation detail.  It must not turn
        ``date and id`` into ``id, date`` or move a later requested attribute
        ahead of an earlier one.  Reordering is performed only when every
        binding has one distinct lexical position after removing its owning
        table's identifier tokens; ambiguous phrases retain their stable
        existing order and therefore fail closed at the ordinary projection
        gate rather than guessing.
        """
        if len(bindings) < 2 or any(
            not isinstance(item, dict)
            or not item.get("table") or not item.get("column")
            for item in bindings
        ):
            return list(bindings)

        expanded_phrase = re.sub(
            r"([a-z0-9])([A-Z])", r"\1 \2", str(output_phrase or ""),
        )
        token_positions: List[set[str]] = [
            self._normalized_language_tokens(token)
            for token in re.findall(r"[A-Za-z0-9]+", expanded_phrase)
        ]
        positions: List[int] = []
        for binding in bindings:
            table_tokens = self._normalized_language_tokens(str(binding["table"]))
            column_tokens = self._normalized_language_tokens(str(binding["column"]))
            distinctive = column_tokens - table_tokens or column_tokens
            matching = [
                index for index, tokens in enumerate(token_positions)
                if tokens & distinctive
            ]
            if not matching:
                return list(bindings)
            positions.append(min(matching))
        if len(set(positions)) != len(positions):
            return list(bindings)
        return [
            item for _position, _index, item in sorted(
                (position, index, binding)
                for index, (position, binding) in enumerate(zip(positions, bindings))
            )
        ]

    @staticmethod
    def _top_level_projection(sql: str) -> Optional[str]:
        """Return the outermost SELECT list while ignoring CTE/subquery SELECTs.

        ``_sql_code_only`` preserves character positions while masking quoted
        values, identifiers and comments.  Tracking parenthesis depth on that
        probe is enough for the bounded projection validators to distinguish a
        final CTE query from its inner SELECT statements without pretending to
        be a full SQL parser.
        """
        source = str(sql or "")
        code = _sql_code_only(source)
        depth = 0
        select_end: Optional[int] = None
        index = 0
        while index < len(code):
            char = code[index]
            if char == "(":
                depth += 1
                index += 1
                continue
            if char == ")":
                depth = max(0, depth - 1)
                index += 1
                continue
            if depth == 0 and (char.isalpha() or char == "_"):
                match = re.match(r"[A-Za-z_][A-Za-z0-9_$]*", code[index:])
                if match is not None:
                    word = match.group(0).casefold()
                    end = index + len(match.group(0))
                    if select_end is None and word == "select":
                        select_end = end
                    elif select_end is not None and word == "from":
                        return source[select_end:index].strip()
                    index = end
                    continue
            index += 1
        return None

    @staticmethod
    def _simple_projection_columns(sql: str) -> List[str]:
        """Return standalone columns from the outermost SELECT projection.

        Expressions are deliberately excluded: an explicit dictionary tuple
        such as ``full name -> first_name, last_name`` is a multi-output
        contract, so concatenating both inputs into one expression does not
        satisfy it. CTE bodies are skipped; set-operation shapes remain outside
        this bounded check.
        """
        projection = NL2SQLExecutor._top_level_projection(sql)
        if projection is None:
            return []
        simple_column = re.compile(
            r"^(?:DISTINCT\s+)?(?:[A-Za-z_][\w$]*\s*\.\s*)?"
            r"(?:\"([^\"]+)\"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))"
            r"(?:\s+AS\s+(?:\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*))?$",
            re.IGNORECASE,
        )
        columns: List[str] = []
        for item in NL2SQLExecutor._split_projection(projection):
            column_match = simple_column.fullmatch(item.strip())
            if column_match is None:
                continue
            raw = next(value for value in column_match.groups() if value is not None)
            folded = raw.casefold()
            if folded not in columns:
                columns.append(folded)
        return columns

    def _compile_relational_contract(self, question: str) -> RelationalAlgebraContract:
        """Compile high-confidence relational requirements without model output."""
        raw_question = str(question or "").strip()
        text, _, evidence_text = raw_question.partition(
            "Relevant business evidence supplied by the user:"
        )
        output_phrase = self._output_request_phrase(raw_question)
        output_tokens = self._normalized_language_tokens(output_phrase)
        contract = RelationalAlgebraContract()

        def require(operator: str, reason: str) -> None:
            if operator not in contract.required_operators:
                contract.required_operators.append(operator)
            if reason not in contract.evidence:
                contract.evidence.append(reason)

        def entity_identity_columns(table_name: str) -> List[str]:
            table = self.schema.tables.get(table_name)
            if table is None:
                return []
            return [
                f"{table_name}.{column.name}"
                for column in table.columns if column.pk
            ]

        def declare_result_grain(
            *,
            kind: str,
            owner_table: str = "",
            identity_columns: Optional[List[str]] = None,
            visible_bindings: Optional[List[dict]] = None,
            cardinality: str = "set",
            multiplicity: str = "one_row_per_entity",
            source: str,
        ) -> bool:
            """Publish one internally consistent result identity contract.

            Display columns and relational identity are deliberately separate:
            a name may be visible without being a safe GROUP BY key.  Multiple
            independent compilers may discover the same shape, but a conflict
            never silently overwrites the earlier authority.
            """
            identities = list(dict.fromkeys(identity_columns or []))
            visible = [
                f"{item.get('table')}.{item.get('column')}"
                for item in (visible_bindings or [])
                if item.get("table") and item.get("column")
            ]
            proposed = {
                "kind": kind,
                "owner_table": owner_table,
                "identity_columns": identities,
                "visible_columns": visible,
                "cardinality": cardinality,
                "multiplicity": multiplicity,
                "source": source,
            }
            if not contract.result_grain:
                contract.result_grain = proposed
                require("result_grain", source)
                return True
            current = dict(contract.result_grain)
            comparable = {
                key: current.get(key)
                for key in (
                    "kind", "owner_table", "identity_columns", "cardinality",
                    "multiplicity",
                )
            }
            expected = {
                key: proposed.get(key)
                for key in comparable
            }
            if comparable != expected:
                ambiguity = {
                    "kind": "result_grain_conflict",
                    "existing": current,
                    "proposed": proposed,
                }
                if ambiguity not in contract.ambiguities:
                    contract.ambiguities.append(ambiguity)
                return False
            if not current.get("visible_columns") and visible:
                current["visible_columns"] = visible
                contract.result_grain = current
            return True

        def declare_aggregate_subject(
            *,
            role: str,
            function: str,
            source_table: str,
            column: str,
            multiplicity: str,
            group_grain: Optional[List[str]] = None,
            source: str,
        ) -> None:
            subject = {
                "role": role,
                "function": function.upper(),
                "source_table": source_table,
                "column": column,
                "multiplicity": multiplicity,
                "group_grain": list(dict.fromkeys(group_grain or [])),
                "source": source,
            }
            if subject not in contract.aggregate_subjects:
                contract.aggregate_subjects.append(subject)
                require("aggregate_subject", source)

        def phrase_column_refs(value: str) -> List[tuple[str, str]]:
            """Map a bounded output/measure phrase to high-overlap schema columns."""
            aliases = {
                "avg": "average", "num": "number", "scr": "score",
                "consume": "consumption", "consumed": "consumption",
            }

            def expanded_tokens(raw: str) -> set[str]:
                tokens = self._normalized_language_tokens(raw)
                expanded = set(tokens)
                for token in list(tokens):
                    if token in aliases:
                        expanded.add(aliases[token])
                    for short, long_name in aliases.items():
                        if token == long_name:
                            expanded.add(short)
                # Preserve common physical measurement abbreviations as
                # schema concepts.  The reverse expansion is important: a
                # question normally spells out "miles per gallon", while the
                # database exposes only ``MPG``.
                if "mpg" in tokens:
                    expanded.update({"miles", "per", "gallon"})
                if {"miles", "per", "gallon"}.issubset(tokens):
                    expanded.add("mpg")
                return expanded

            phrase_tokens = expanded_tokens(value)
            matches: List[tuple[float, int, int, str, str]] = []
            for table_index, table in enumerate(self.schema.tables.values()):
                for column_index, column in enumerate(table.columns):
                    physical = expanded_tokens(column.name)
                    metadata = expanded_tokens(" ".join((
                        column.semantic_name, column.description,
                    )))
                    concept = physical | metadata
                    overlap = concept & phrase_tokens
                    direct = bool(physical and physical.issubset(phrase_tokens))
                    described = bool(
                        len(overlap) >= 2
                        and len(overlap) / max(1, min(len(concept), len(phrase_tokens))) >= 0.4
                    )
                    if direct or described:
                        score = (
                            100.0 + len(physical)
                            if direct else 10.0 * len(overlap)
                            + len(overlap) / max(1, len(concept))
                        )
                        matches.append((
                            score, table_index, column_index, table.name, column.name,
                        ))
            if not matches:
                return []
            best_score = max(item[0] for item in matches)
            return [
                (item[3], item[4])
                for item in sorted(matches, key=lambda entry: (entry[1], entry[2]))
                if item[0] == best_score
            ]

        def phrase_columns(value: str) -> List[str]:
            return [column for _table, column in phrase_column_refs(value)]

        # Numeric business semantics and physical storage types are separate
        # authorities.  A model must not silently turn a native numeric column
        # into another type, nor cast dirty TEXT to a number without defining
        # what happens to invalid values (SQLite otherwise turns many strings
        # into zero).  Bind only a closed aggregate/comparison grammar and one
        # uniquely matched measure column.
        numeric_measure_phrase = re.search(
            r"\b(?:minimum|maximum|average|mean|sum|total)\s+(?:of\s+)?"
            r"(?P<measure>[A-Za-z][\w-]*(?:\s+per\s+[A-Za-z][\w-]*){0,2})\b",
            text,
            re.IGNORECASE,
        )
        if numeric_measure_phrase is None:
            numeric_measure_phrase = re.search(
                r"\b(?P<measure>[A-Za-z][\w-]*(?:\s+per\s+"
                r"[A-Za-z][\w-]*){0,2})\s+"
                r"(?:below|above|under|over|less\s+than|greater\s+than)\s+"
                r"(?:the\s+)?(?:average|mean)\b",
                text,
                re.IGNORECASE,
            )
        if numeric_measure_phrase is not None:
            measure_phrase = numeric_measure_phrase.group("measure")
            measure_refs = phrase_column_refs(measure_phrase)
            generic_count_measure = self._normalized_language_tokens(
                measure_phrase
            ).issubset({"count", "number"})
            if len(measure_refs) == 1 and not generic_count_measure:
                measure_table, measure_column = measure_refs[0]
                column = next((
                    item for item in self.schema.tables[measure_table].columns
                    if item.name == measure_column
                ), None)
                concept_tokens = self._normalized_language_tokens(" ".join((
                    measure_phrase,
                    measure_column,
                    str(column.semantic_name if column else ""),
                    str(column.description if column else ""),
                )))
                numeric_concepts = {
                    "age", "amount", "area", "balance", "cost", "count",
                    "distance", "duration", "earning", "height", "horsepower",
                    "income", "length", "mpg", "number", "percentage", "price",
                    "quantity", "rate", "rating", "salary", "score", "speed",
                    "total", "volume", "weight",
                }
                physical_numeric = bool(column and re.search(
                    r"(?:INT|REAL|NUMERIC|DECIMAL|FLOAT|DOUBLE|NUMBER)",
                    column.type or "", re.IGNORECASE,
                ))
                sampled_numeric = bool(
                    column and len(column.sample_values) >= 2
                    and all(re.fullmatch(
                        r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?",
                        str(value).strip(),
                    ) for value in column.sample_values)
                )
                if physical_numeric or sampled_numeric \
                        or bool(concept_tokens & numeric_concepts):
                    requirement = {
                        "column": f"{measure_table}.{measure_column}",
                        "semantic_type": "numeric",
                        "physical_type": str(column.type if column else ""),
                        "coercion": (
                            "native_numeric" if physical_numeric
                            else "controlled_numeric_parse"
                        ),
                        "invalid_value_policy": (
                            "not_applicable" if physical_numeric else "exclude"
                        ),
                        "source": "numeric_operator_bound_to_physical_measure",
                    }
                    contract.value_domain_requirements.append(requirement)
                    require("value_domain", requirement["source"])

        def lexical_roots(value: str) -> set[str]:
            """Small, auditable morphology bridge for schema/table mentions."""
            roots: set[str] = set()
            aliases = {
                "consume": "consumption", "consumed": "consumption",
                "visiting": "visit", "visited": "visit",
            }
            for token in self._normalized_language_tokens(value):
                roots.add(aliases.get(token, token))
                if len(token) > 4 and token.endswith("ing"):
                    roots.add(token[:-3])
                if len(token) > 3 and token.endswith("ed"):
                    roots.update({token[:-2], token[:-1]})
            return roots

        def mentioned_column_bindings(value: str) -> List[tuple[str, str]]:
            """Resolve exact physical column mentions that have one owning table."""
            owners: Dict[str, List[str]] = {}
            canonical_columns: Dict[tuple[str, str], str] = {}
            mentioned: List[tuple[str, str]] = []
            for table in self.schema.tables.values():
                for column in table.columns:
                    owners.setdefault(column.name.casefold(), []).append(table.name)
                    canonical_columns[(table.name, column.name.casefold())] = column.name
                    if re.search(
                        rf"(?<![A-Za-z0-9_])(?:[`\"\[]?{re.escape(table.name)}"
                        rf"[`\"\]]?)\s*\.\s*(?:[`\"\[]?{re.escape(column.name)}"
                        rf"[`\"\]]?)(?![A-Za-z0-9_])",
                        value,
                        re.IGNORECASE,
                    ):
                        mentioned.append((table.name, column.name))
            for folded, table_names in owners.items():
                if len(table_names) != 1 or not re.search(
                    rf"(?<![A-Za-z0-9_]){re.escape(folded)}(?![A-Za-z0-9_])",
                    value.casefold(),
                ):
                    continue
                binding = (
                    table_names[0], canonical_columns[(table_names[0], folded)],
                )
                if binding not in mentioned:
                    mentioned.append(binding)
            return mentioned

        def mentioned_tables_from_columns(value: str) -> List[str]:
            return list(dict.fromkeys(
                table for table, _column in mentioned_column_bindings(value)
            ))

        def named_tables(value: str) -> List[str]:
            roots = lexical_roots(value)
            scored: List[tuple[tuple[int, int, int], int, str]] = []
            for index, table in enumerate(self.schema.tables.values()):
                table_roots = lexical_roots(table.name) - {
                    "has", "have", "link", "map", "relation", "xref",
                }
                overlap = table_roots & roots
                if not table_roots or not overlap:
                    continue
                # A physical table named ``show`` must not win merely because
                # an imperative request starts with "Show ...".  It remains a
                # valid table mention when the noun occurs anywhere else.
                if table_roots == {"show"} and re.match(
                    r"^\s*show\b", value, re.IGNORECASE,
                ) and not re.search(
                    r"\bshow\b", re.sub(r"^\s*show\b", "", value, count=1,
                                         flags=re.IGNORECASE), re.IGNORECASE,
                ):
                    continue
                # Prefer complete physical-name coverage before raw overlap.
                # This keeps ``concert`` ahead of ``singer_in_concert`` for a
                # phrase containing only "concerts", and ``Templates`` ahead
                # of ``Ref_Template_Types`` when the reference-table prefix was
                # never stated.  Partial lexical overlap remains a fallback.
                score = (
                    1 if table_roots.issubset(roots) else 0,
                    len(overlap),
                    int(1000 * len(overlap) / len(table_roots)),
                )
                scored.append((score, index, table.name))
            if not scored:
                return []
            best = max(item[0] for item in scored)
            return [
                item[2] for item in sorted(scored, key=lambda entry: entry[1])
                if item[0] == best
            ]

        def explicit_named_tables(value: str) -> List[str]:
            """Return tables whose meaningful name tokens are all in the text.

            ``named_tables`` intentionally supports fuzzy single-token routing for
            a bounded phrase.  Relation-path compilation needs a stronger signal:
            every meaningful table token must be present.  Generic UI/query verbs
            are excluded so a physical table named ``show`` is not selected merely
            because the user started the request with "Show ...".
            """
            roots = lexical_roots(value)
            generic = {
                "all", "code", "count", "data", "date", "detail", "item", "list",
                "description", "name", "number", "order", "record", "result",
                "show", "time", "type", "value", "year",
            }
            matched: List[str] = []
            for table in self.schema.tables.values():
                table_roots = lexical_roots(table.name) - {
                    "has", "have", "link", "map", "relation", "xref",
                }
                meaningful = table_roots - generic
                if not meaningful or not meaningful.issubset(roots):
                    continue
                expanded = re.sub(
                    r"([a-z0-9])([A-Z])", r"\1 \2", str(table.name),
                )
                ordered = [
                    token.casefold() for token in re.findall(r"[A-Za-z0-9]+", expanded)
                    if lexical_roots(token) & meaningful
                ]
                if len(ordered) >= 2:
                    contiguous = r"[\s_-]+".join(
                        rf"{re.escape(token)}(?:s|es)?" for token in ordered
                    )
                    if re.search(rf"(?<![A-Za-z0-9_]){contiguous}(?![A-Za-z0-9_])", value, re.IGNORECASE):
                        matched.append(table.name)
                    continue
                root = next(iter(meaningful))
                physical_tokens = [
                    token.casefold()
                    for token in re.findall(r"[A-Za-z0-9]+", expanded)
                    if lexical_roots(token) & meaningful
                ]
                variants = {rf"{re.escape(root)}(?:s|es)?"}
                variants.update(re.escape(token) for token in physical_tokens)
                occurrences = list(re.finditer(
                    rf"(?<![A-Za-z0-9_])(?:{'|'.join(sorted(variants))})(?![A-Za-z0-9_])",
                    value,
                    re.IGNORECASE,
                ))
                for occurrence in occurrences:
                    following = re.match(
                        r"[\s_-]+(?P<next>[A-Za-z][\w-]*)",
                        value[occurrence.end():],
                    )
                    if following and lexical_roots(following.group("next")) & generic:
                        # ``course results`` and ``series name`` are column/
                        # business phrases unless that trailing concept is a
                        # real column on this same table (``parent names``).
                        following_roots = lexical_roots(following.group("next"))
                        has_same_table_column = any(
                            bool(
                                self._normalized_language_tokens(column.name)
                                & following_roots
                            )
                            for column in table.columns
                        )
                        if not has_same_table_column:
                            continue
                    matched.append(table.name)
                    break
            return matched

        def exact_projection_bindings(value: str, table_name: str) -> List[dict]:
            """Bind a closed output phrase to physical columns on one table.

            This deliberately uses physical identifier tokens only.  A column
            is accepted when all its tokens occur in the output phrase and all
            remaining meaningful phrase tokens are accounted for by the query
            frame, entity table or selected columns.  Descriptions and sample
            values are excluded because a deterministic compiler must not turn
            fuzzy semantic similarity into an executable projection.
            """
            table = self.schema.tables.get(table_name)
            if table is None:
                return []
            phrase_tokens = self._normalized_language_tokens(value)
            if not phrase_tokens:
                return []
            matches: List[dict] = []
            covered: set[str] = set()
            for column in table.columns:
                column_tokens = self._normalized_language_tokens(column.name)
                if column_tokens and column_tokens.issubset(phrase_tokens):
                    matches.append({"table": table_name, "column": column.name})
                    covered.update(column_tokens)
            if not matches:
                return []
            frame_tokens = {
                "a", "address", "adress", "all", "also", "an", "and", "are",
                "each", "find", "for", "full", "give", "is", "list", "of",
                "one", "per", "please", "return", "show", "the", "what",
                "which", "who",
            }
            residual = phrase_tokens - frame_tokens
            residual -= self._normalized_language_tokens(table_name)
            residual -= covered
            # Some schemas use an eponymous business label (table
            # ``orchestra``, column ``Orchestra``) instead of a generic
            # ``Name`` column.  Once that exact physical label is among the
            # requested matches, a user's generic word "name" is a request
            # role rather than an unbound physical token.
            matched_columns = [
                column for column in table.columns
                if any(column.name == item["column"] for item in matches)
            ]
            if any(
                self._normalized_language_tokens(column.name).issubset(
                    self._normalized_language_tokens(table_name)
                )
                for column in matched_columns
            ):
                residual -= {"name"}
            return [] if residual else matches

        def unique_fk_path(start: str, end: str, max_edges: int = 4) -> Optional[dict]:
            """Return one unique shortest physical FK path, including its columns."""
            if start == end or start not in self.schema.tables or end not in self.schema.tables:
                return None
            canonical = {name.casefold(): name for name in self.schema.tables}
            adjacency: Dict[str, List[tuple[str, dict]]] = {
                name: [] for name in self.schema.tables
            }
            for table in self.schema.tables.values():
                for column in table.columns:
                    target = canonical.get(str(column.fk_table or "").casefold())
                    if not target or not column.fk_column:
                        continue
                    edge = {
                        "from": f"{table.name}.{column.name}",
                        "to": f"{target}.{column.fk_column}",
                        "source": "foreign_key",
                    }
                    adjacency[table.name].append((target, edge))
                    adjacency[target].append((table.name, edge))
            found: List[tuple[List[str], List[dict]]] = []

            def walk(current: str, tables: List[str], edges: List[dict]) -> None:
                if len(edges) > max_edges:
                    return
                if current == end:
                    found.append((tables, edges))
                    return
                for neighbor, edge in adjacency.get(current, []):
                    if neighbor not in tables:
                        walk(neighbor, [*tables, neighbor], [*edges, edge])

            walk(start, [start], [])
            if not found:
                return None
            shortest = min(len(item[1]) for item in found)
            candidates = [item for item in found if len(item[1]) == shortest]
            fingerprints = {
                tuple(
                    tuple(sorted((
                        str(edge["from"]).casefold(), str(edge["to"]).casefold(),
                    )))
                    for edge in edges
                )
                for _tables, edges in candidates
            }
            if len(candidates) != 1 or len(fingerprints) != 1:
                return None
            tables, edges = candidates[0]
            return {
                "tables": tables,
                "edges": edges,
                "source": "unique_shortest_declared_fk_path",
            }

        def nearest_column_binding(
            refs: List[tuple[str, str]], anchor_tables: List[str],
        ) -> Optional[tuple[tuple[str, str], Optional[dict]]]:
            """Choose one schema column by a unique shortest declared-FK distance."""
            candidates: List[tuple[int, int, tuple[str, str], Optional[dict]]] = []
            seen: set[tuple[str, str]] = set()
            for ref_index, ref in enumerate(refs):
                if ref in seen:
                    continue
                seen.add(ref)
                table_name, _column_name = ref
                distances: List[tuple[int, Optional[dict]]] = []
                for anchor in anchor_tables:
                    if anchor == table_name:
                        distances.append((0, None))
                        continue
                    path = unique_fk_path(anchor, table_name)
                    if path is not None:
                        distances.append((len(path.get("edges") or []), path))
                if distances:
                    distance, path = min(distances, key=lambda item: item[0])
                    candidates.append((distance, ref_index, ref, path))
            if not candidates:
                return None
            best_distance = min(item[0] for item in candidates)
            nearest = [item for item in candidates if item[0] == best_distance]
            if len(nearest) != 1:
                return None
            return nearest[0][2], nearest[0][3]

        # Imperative "rank ... by ..., showing ..." asks for a rank value, not
        # merely an ordered list. Other uses of "rank" remain untouched.
        rank_request = re.search(
            r"^\s*rank\b.+\bby\b.+\bshowing\b", text,
            re.IGNORECASE | re.DOTALL,
        )
        if rank_request or re.search(r"(?:排名|排行).+(?:显示|给出).+(?:名次|排名)", text):
            require("rank_projection", "explicit_rank_result")
        if rank_request:
            showing = re.search(r"\bshowing\s+(.+?)(?:[?.]|$)", text, re.IGNORECASE | re.DOTALL)
            measure = re.search(
                r"^\s*rank\b.+?\bby\s+(.+?)(?=\bwhere\b|\bshowing\b|[?.]|$)",
                text, re.IGNORECASE | re.DOTALL,
            )
            for phrase in (
                showing.group(1) if showing else "",
                measure.group(1) if measure else "",
            ):
                for column_name in phrase_columns(phrase):
                    if column_name not in contract.output_columns:
                        contract.output_columns.append(column_name)

        grouped_measure = bool(re.search(
            r"\b(?:how\s+many|number\s+of|average|avg|total|sum|count|percentage|ratio)\b|"
            r"多少|数量|平均|总计|合计|比例|百分比",
            text, re.IGNORECASE,
        ))
        if grouped_measure:
            # ``per`` is also part of physical units (miles per gallon, cost
            # per litre).  It becomes an entity-grain operator only when the
            # following phrase resolves to exactly one physical table.  This
            # prevents an ordinary global AVG from acquiring a false GROUP BY
            # contract merely because its measure contains a unit ratio.
            entity_match = re.search(
                r"\b(?:for\s+each|per)\s+(?:the\s+)?"
                r"(?P<entity>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*){0,2}?)"
                r"(?=\s+\b(?:and|with|who|that|where|having)\b|[,?.]|$)",
                text,
                re.IGNORECASE,
            )
            if entity_match:
                entity_tokens = self._normalized_language_tokens(
                    entity_match.group("entity")
                )
                entity_tables = [
                    table for table in self.schema.tables.values()
                    if self._normalized_language_tokens(table.name)
                    and self._normalized_language_tokens(table.name).issubset(entity_tokens)
                ]
                if len(entity_tables) == 1:
                    require("group_by", "per_entity_measure")
                    for column in entity_tables[0].columns:
                        if column.pk:
                            contract.grouping_keys.append(
                                f"{entity_tables[0].name}.{column.name}"
                            )
                    if contract.grouping_keys:
                        require("grouping_entity_key", "declared_entity_primary_key")
            elif re.search(r"按每个|逐个", text):
                require("group_by", "per_entity_measure")

        # A direct average request with one unambiguous physical measure gets
        # an aggregate-input contract.  This intentionally excludes superlative
        # phrases such as "highest average salary", where "average salary" may
        # be a precomputed business attribute rather than an AVG operation.
        average_match = re.search(
            r"\baverage\s+(?:of\s+)?(?P<measure>.+?)"
            r"(?=\s+\b(?:for\s+each|for\s+all|of\s+all|per|by|where|whose|"
            r"that|with|having)\b|[,?.]|$)",
            text,
            re.IGNORECASE,
        )
        if average_match and (
            re.search(r"\b(?:for\s+each|per)\b", text, re.IGNORECASE)
            or re.search(
                r"\b(?:what\s+is|calculate|find|return|show|give)\s+(?:the\s+)?average\b",
                text,
                re.IGNORECASE,
            )
        ) and not re.search(
            r"\b(?:highest|lowest|largest|smallest|maximum|minimum)\s+average\b",
            text,
            re.IGNORECASE,
        ):
            refs = phrase_column_refs(average_match.group("measure"))
            # The first bounded match deliberately stops at ``per`` because it
            # may introduce an entity grain ("average salary per department").
            # If that prefix is not a physical measure, try the complete unit
            # phrase before the next real clause boundary.  This keeps
            # "miles per gallon" and similar schema-bound units intact without
            # turning an arbitrary trailing phrase into an aggregate input.
            if len(refs) != 1 and re.search(
                r"\bper\b", text[average_match.start("measure"):], re.IGNORECASE,
            ):
                unit_measure_match = re.search(
                    r"\baverage\s+(?:of\s+)?(?P<measure>.+?)"
                    r"(?=\s+\b(?:for\s+each|for\s+all|of\s+all|by|where|whose|"
                    r"that|with|having)\b|[,?.]|$)",
                    text,
                    re.IGNORECASE,
                )
                if unit_measure_match:
                    unit_refs = phrase_column_refs(unit_measure_match.group("measure"))
                    if len(unit_refs) == 1:
                        refs = unit_refs
            if len(refs) == 1:
                table_name, column_name = refs[0]
                contract.aggregate_requirements.append({
                    "function": "AVG",
                    "column": f"{table_name}.{column_name}",
                })
                declare_aggregate_subject(
                    role="visible_measure",
                    function="AVG",
                    source_table=table_name,
                    column=column_name,
                    multiplicity="measure_values",
                    group_grain=list(contract.grouping_keys),
                    source="explicit_average_measure_subject",
                )
                require("aggregate_input", "explicit_average_measure")

        if re.search(
            r"\b(?:distinct|unique)\b.{0,48}\b(?:how\s+many|number|count)\b|"
            r"\bhow\s+many\b.{0,48}\b(?:distinct|unique)\b|去重.+(?:数量|多少)|"
            r"(?:多少|数量).+去重",
            text, re.IGNORECASE | re.DOTALL,
        ):
            require("distinct_entity_count", "explicit_distinct_count")

        threshold_relation = re.search(
            r"\b(?:who|which|that)\b.{0,80}\b(?:has|have|had|make|makes|made|"
            r"produce|produces|produced|conduct|conducted|win|won)\b.{0,40}"
            r"\b(?:at\s+least|more\s+than|fewer\s+than|less\s+than|exactly)\s+"
            r"(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+"
            r"[A-Za-z][\w-]*s\b",
            text, re.IGNORECASE | re.DOTALL,
        )
        number_words = {
            "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
            "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
        }
        operator_by_phrase = {
            "at least": ">=", "more than": ">", "fewer than": "<",
            "less than": "<", "exactly": "=",
        }

        def scalar_threshold_binding(subject: str) -> Optional[tuple[str, str]]:
            subject_tokens = self._normalized_language_tokens(subject)
            candidates: List[tuple[str, str]] = []
            counter_tokens = {"count", "num", "number", "quantity", "total"}
            for table in self.schema.tables.values():
                for column in table.columns:
                    column_tokens = self._normalized_language_tokens(column.name)
                    if (
                        subject_tokens
                        and subject_tokens.issubset(column_tokens)
                        and (column_tokens - subject_tokens).issubset(counter_tokens)
                    ):
                        candidates.append((table.name, column.name))
            return candidates[0] if len(candidates) == 1 else None

        relation_threshold_found = False
        for threshold in re.finditer(
            r"\b(?P<operator>at\s+least|more\s+than|fewer\s+than|"
            r"less\s+than|exactly)\s+"
            r"(?P<value>\d+|one|two|three|four|five|six|seven|eight|nine|ten)"
            r"\s+(?P<subject>[A-Za-z][\w-]*s)\b",
            text,
            re.IGNORECASE,
        ):
            raw_value = threshold.group("value").casefold()
            value = int(raw_value) if raw_value.isdigit() else number_words[raw_value]
            operator = operator_by_phrase[
                re.sub(r"\s+", " ", threshold.group("operator").casefold())
            ]
            subject = threshold.group("subject").casefold()
            scalar_binding = scalar_threshold_binding(subject)
            if scalar_binding is not None:
                table_name, column_name = scalar_binding
                requirement = {
                    "column": f"{table_name}.{column_name}",
                    "operator": operator,
                    "value": value,
                    "value_type": "number",
                    "scope": (
                        "group_member_predicate" if re.search(
                            r"\bboth\b.+\band\b", text, re.IGNORECASE | re.DOTALL,
                        ) else "row_predicate"
                    ),
                }
                if requirement not in contract.filter_requirements:
                    contract.filter_requirements.append(requirement)
                require("typed_filter", "scalar_attribute_threshold")
            elif threshold_relation:
                contract.relationship_thresholds.append({
                    "operator": operator,
                    "value": value,
                    "subject": subject,
                })
                relation_threshold_found = True
        if relation_threshold_found:
            require("group_by", "relationship_count_threshold")
            require("having", "relationship_count_threshold")

        comparison = re.search(
            r"\b(?P<direction>greater|larger|higher|more|less|lower|smaller|fewer)\b"
            r".{0,80}?\bthan\s+(?P<quantifier>any|all|every)\b",
            text, re.IGNORECASE | re.DOTALL,
        )
        if comparison:
            direction = comparison.group("direction").casefold()
            quantifier = comparison.group("quantifier").casefold()
            contract.comparison_direction = (
                "greater" if direction in {"greater", "larger", "higher", "more"}
                else "less"
            )
            contract.comparison_quantifier = (
                "existential_any" if quantifier == "any" else "universal_all"
            )
            require("quantified_comparison", "explicit_any_all_quantifier")

        # Explicit public/business dictionary tuples are authoritative output
        # atoms. Accept both parenthesized and comma-separated mappings, but
        # only when at least two physical columns are named on the RHS.
        schema_columns = {
            column.name.casefold(): column.name
            for table in self.schema.tables.values()
            for column in table.columns
        }
        schema_column_bindings: Dict[str, List[tuple[str, str]]] = {}
        for table in self.schema.tables.values():
            for column in table.columns:
                schema_column_bindings.setdefault(column.name.casefold(), []).append(
                    (table.name, column.name)
                )
        entity_output_tokens = {
            "account", "author", "customer", "driver", "employee", "maker", "member",
            "patient", "person", "player", "professional", "school", "student", "teacher",
            "team", "user", "visitor",
        }
        mapped_output_columns: List[str] = []
        mapped_output_bindings: List[dict] = []
        for clause in re.split(r"[;\r\n]+", evidence_text):
            mapping = re.match(r"\s*(.+?)\s+refers\s+to\s+(.+?)\s*$", clause, re.IGNORECASE)
            if mapping is None:
                continue
            left = mapping.group(1).strip(" '`\"")
            right = mapping.group(2).strip().strip("()")
            left_tokens = self._normalized_language_tokens(left)
            requested_left_tokens = left_tokens - {
                "a", "all", "an", "give", "list", "return", "show", "the",
            }
            names_requested_entity = bool(
                {"full", "name"}.issubset(left_tokens)
                and left_tokens & output_tokens & entity_output_tokens
            )
            if not left or not (
                left.casefold() in output_phrase.casefold()
                or (
                    requested_left_tokens
                    and requested_left_tokens.issubset(output_tokens)
                )
                or names_requested_entity
            ):
                continue
            matched_columns: List[str] = []
            occupied_spans: List[tuple[int, int]] = []
            for folded, canonical in sorted(
                schema_columns.items(), key=lambda item: len(item[0]), reverse=True,
            ):
                occurrence = re.search(
                    rf"(?<![A-Za-z0-9_]){re.escape(folded)}(?![A-Za-z0-9_])",
                    right.casefold(),
                )
                if occurrence is None or canonical in matched_columns:
                    continue
                span = occurrence.span()
                if any(span[0] < used[1] and used[0] < span[1] for used in occupied_spans):
                    continue
                matched_columns.append(canonical)
                occupied_spans.append(span)
            for canonical in matched_columns:
                if canonical not in mapped_output_columns:
                    mapped_output_columns.append(canonical)
                possible_bindings = schema_column_bindings.get(canonical.casefold(), [])
                explicit_bindings = [
                    (table_name, column_name)
                    for table_name, column_name in possible_bindings
                    if re.search(
                        rf"(?<![A-Za-z0-9_])(?:[`\"\[]?{re.escape(table_name)}[`\"\]]?)"
                        rf"\s*\.\s*(?:[`\"\[]?{re.escape(column_name)}[`\"\]]?)"
                        rf"(?![A-Za-z0-9_])",
                        right,
                        re.IGNORECASE,
                    )
                ]
                resolved_binding = (
                    explicit_bindings[0] if len(explicit_bindings) == 1
                    else possible_bindings[0] if len(possible_bindings) == 1
                    else None
                )
                if resolved_binding is not None:
                    binding = {
                        "table": resolved_binding[0],
                        "column": resolved_binding[1],
                    }
                    if binding not in mapped_output_bindings:
                        mapped_output_bindings.append(binding)
                elif len(possible_bindings) > 1:
                    unresolved_binding = {
                        "table": "",
                        "column": canonical,
                        "table_candidates": [item[0] for item in possible_bindings],
                    }
                    if unresolved_binding not in mapped_output_bindings:
                        mapped_output_bindings.append(unresolved_binding)
            if len(matched_columns) >= 2 and matched_columns not in contract.output_bundles:
                contract.output_bundles.append(matched_columns[:8])
                require("separate_projection_atoms", "explicit_dictionary_tuple")
        if contract.output_bundles:
            for canonical in mapped_output_columns:
                if canonical not in contract.output_columns:
                    contract.output_columns.append(canonical)
            for binding in mapped_output_bindings:
                if binding["column"] in contract.output_columns \
                        and binding not in contract.output_bindings:
                    contract.output_bindings.append(binding)

        # A schema flag that lexically modifies another requested output
        # ("official language", "active customer") is a predicate, not an
        # additional output, when both columns coexist on the same table.
        modifiers = {"active", "approved", "enabled", "official", "public", "valid"}
        for table in self.schema.tables.values():
            for flag in table.columns:
                flag_tokens = self._normalized_language_tokens(flag.name)
                matched_modifiers = flag_tokens & modifiers & output_tokens
                if not matched_modifiers or not (
                    flag_tokens & {"flag", "is", "status"}
                    or flag.name.casefold().startswith(("is_", "is"))
                ):
                    continue
                for value_column in table.columns:
                    if value_column is flag:
                        continue
                    value_tokens = self._normalized_language_tokens(value_column.name)
                    if value_tokens and value_tokens.issubset(output_tokens - matched_modifiers):
                        qualified = f"{table.name}.{flag.name}"
                        if value_column.name not in contract.output_columns:
                            contract.output_columns.append(value_column.name)
                        output_binding = {
                            "table": table.name,
                            "column": value_column.name,
                        }
                        if output_binding not in contract.output_bindings:
                            contract.output_bindings.append(output_binding)
                        if qualified not in contract.modifier_filters:
                            contract.modifier_filters.append(qualified)
                            require("modifier_filter", "adjective_maps_to_schema_flag")
                        break

        # A ratio is not merely arithmetic: its denominator defines the
        # population.  Bind high-confidence physical scope tables locally so
        # a repair cannot silently switch from a filtered fact population to a
        # dimension-wide count (the recurrent BIRD denominator failure).
        ratio_match = re.search(
            r"\b(?:percentage|percent|proportion|ratio)\b|百分比|比例",
            text,
            re.IGNORECASE,
        )
        if ratio_match:
            comparison_measure = re.search(
                r"(?P<measure>[A-Za-z][A-Za-z0-9_ -]{0,48}?)\s+"
                r"(?:more\s+than|greater\s+than|above|over|less\s+than|"
                r"below|under|at\s+least|at\s+most)\s+[-+]?\d",
                text,
                re.IGNORECASE,
            )
            comparison_refs = phrase_column_refs(
                comparison_measure.group("measure") if comparison_measure else ""
            )
            entity_match = re.search(
                r"\b(?:percentage|percent|proportion|ratio)\s+of\s+"
                r"(?:the\s+)?(?P<entity>[A-Za-z][\w-]*)",
                text,
                re.IGNORECASE,
            )
            entity_tables = named_tables(entity_match.group("entity")) \
                if entity_match else []
            # Population scope starts from bounded question clauses.  The
            # evidence block may contain formula words such as ``COUNT(...)``;
            # treating those as unqualified physical columns can accidentally
            # pull in an unrelated table that happens to own a column named
            # ``Count``.  A pre-ratio population phrase (``Among comments ...``),
            # the explicit comparison measure and the ratio body provide the
            # independent table evidence.  Evidence remains available below
            # only for explicit grain and time-scope bindings.
            scope_tables: List[str] = []

            def add_scope_table(name: str) -> None:
                if name in self.schema.tables and name not in scope_tables:
                    scope_tables.append(name)

            for table_name, _column in comparison_refs:
                add_scope_table(table_name)
            population_prefix = text[:ratio_match.start()]
            prefix_tables = explicit_named_tables(population_prefix)
            for table_name in prefix_tables:
                add_scope_table(table_name)
            if prefix_tables and len(entity_tables) == 1:
                add_scope_table(entity_tables[0])
            elif not scope_tables and len(entity_tables) == 1:
                add_scope_table(entity_tables[0])
            ratio_body = text[ratio_match.end():]
            if not scope_tables:
                for table_name, _column in phrase_column_refs(ratio_body):
                    add_scope_table(table_name)
            if not scope_tables:
                for table_name in explicit_named_tables(text):
                    add_scope_table(table_name)
            # Evidence may name a physical predicate/grain column whose table
            # is not lexicalized in the question (for example
            # ``preferred_foot``).  Add that table only through a unique
            # declared FK path to the question scope.  A token used solely as
            # a function call (``COUNT(``, ``SUM(``, ...) is not column
            # evidence, preventing formula syntax from selecting an unrelated
            # table with a colliding physical column name.
            for table_name, column_name in mentioned_column_bindings(evidence_text):
                if table_name in scope_tables or not scope_tables \
                        or len(scope_tables) >= 3:
                    continue
                occurrences = list(re.finditer(
                    rf"(?<![A-Za-z0-9_]){re.escape(column_name)}"
                    rf"(?![A-Za-z0-9_])",
                    evidence_text,
                    re.IGNORECASE,
                ))
                if occurrences and all(re.match(
                    r"\s*\(", evidence_text[item.end():], re.IGNORECASE,
                ) for item in occurrences):
                    continue
                if any(
                    unique_fk_path(existing, table_name) is not None
                    for existing in scope_tables
                ):
                    add_scope_table(table_name)
            grain = ""
            if len(entity_tables) == 1:
                entity_table = self.schema.tables[entity_tables[0]]
                entity_keys = [column for column in entity_table.columns if column.pk]
                if len(entity_keys) == 1:
                    entity_key = entity_keys[0]
                    if len(scope_tables) == 1 and scope_tables[0] != entity_table.name:
                        fact_keys = [
                            column for column in self.schema.tables[scope_tables[0]].columns
                            if (
                                column.fk_table
                                and column.fk_table.casefold() == entity_table.name.casefold()
                                and column.fk_column
                                and column.fk_column.casefold() == entity_key.name.casefold()
                            ) or column.name.casefold() == entity_key.name.casefold()
                        ]
                        if len(fact_keys) == 1:
                            grain = f"{scope_tables[0]}.{fact_keys[0].name}"
                    elif entity_table.name in scope_tables:
                        grain = f"{entity_table.name}.{entity_key.name}"
            explicit_denominator = re.search(
                r"\bCOUNT\s*\(\s*(?:DISTINCT\s+)?(?P<column>[A-Za-z_][\w$]*)"
                r"(?=\s*\)|\s+where\b)",
                evidence_text,
                re.IGNORECASE,
            )
            if explicit_denominator:
                column_name = explicit_denominator.group("column")
                owners = [
                    table.name for table in self.schema.tables.values()
                    if table.name in scope_tables and any(
                        column.name.casefold() == column_name.casefold()
                        for column in table.columns
                    )
                ]
                if len(owners) == 1 and owners[0] in scope_tables:
                    canonical_column = next(
                        column.name for column in self.schema.tables[owners[0]].columns
                        if column.name.casefold() == column_name.casefold()
                    )
                    grain = f"{owners[0]}.{canonical_column}"

            # At most three exact-evidence tables keeps this a bounded local
            # contract. Larger/ambiguous semantic joins remain model territory.
            if 1 <= len(scope_tables) <= 3:
                # Shared denominator filters require scope evidence, not merely
                # any column mentioned in the numerator or business dictionary.
                # A comparison before the ratio phrase (for example ``Among
                # comments with scores between 5 and 10, what percentage ...``)
                # is a population filter.  Conditions after the ratio keyword
                # are normally numerator/category conditions and are therefore
                # not promoted.  Explicitly bound time columns are shared when
                # the question itself carries a temporal scope.
                base_candidates: List[tuple[str, str]] = []
                for population_comparison in re.finditer(
                    r"(?P<measure>[A-Za-z][A-Za-z0-9_ -]{0,64}?)\s+"
                    r"(?:between\s+[-+]?\d+(?:\.\d+)?\s+(?:and|to)\s+"
                    r"[-+]?\d+(?:\.\d+)?|from\s+[-+]?\d+(?:\.\d+)?\s+"
                    r"to\s+[-+]?\d+(?:\.\d+)?|more\s+than\s+[-+]?\d|"
                    r"greater\s+than\s+[-+]?\d|above\s+[-+]?\d|"
                    r"over\s+[-+]?\d|less\s+than\s+[-+]?\d|"
                    r"below\s+[-+]?\d|under\s+[-+]?\d|"
                    r"at\s+least\s+[-+]?\d|at\s+most\s+[-+]?\d)",
                    population_prefix,
                    re.IGNORECASE,
                ):
                    for binding in phrase_column_refs(
                        population_comparison.group("measure")
                    ):
                        if binding not in base_candidates:
                            base_candidates.append(binding)
                temporal_scope = bool(re.search(
                    r"\b(?:date|day|week|month|quarter|year|january|february|"
                    r"march|april|may|june|july|august|september|october|"
                    r"november|december|\d{4})\b|日期|月份|季度|年份|年度",
                    text,
                    re.IGNORECASE,
                ))
                if temporal_scope:
                    for binding in mentioned_column_bindings(evidence_text):
                        table_name, column_name = binding
                        if table_name not in scope_tables:
                            continue
                        column = next(
                            item for item in self.schema.tables[table_name].columns
                            if item.name == column_name
                        )
                        if is_time_column(column) and binding not in base_candidates:
                            base_candidates.append(binding)
                base_filter_columns: List[str] = []
                grain_folded = grain.casefold()
                for table_name, column_name in base_candidates:
                    if table_name not in scope_tables \
                            or f"{table_name}.{column_name}".casefold() == grain_folded:
                        continue
                    base_filter_columns.append(f"{table_name}.{column_name}")
                kind = "category_ratio" if re.search(
                    r"\b(?:against|versus|vs\.?|compared\s+with)\b",
                    text,
                    re.IGNORECASE,
                ) else "conditional_share"
                contract.ratio_requirements.append({
                    "kind": kind,
                    "population_tables": scope_tables,
                    "denominator_grain": grain,
                    "base_filter_columns": base_filter_columns,
                    "shared_scope": "same_relations_and_base_filters",
                    "scale": 100 if re.search(
                        r"\b(?:percentage|percent)\b|百分比", text, re.IGNORECASE,
                    ) else 1,
                })
                require("ratio_population", "ratio_denominator_population")

        # Compile direct anti-relationships only when both endpoint tables and
        # a single FK path are provable. Global scalar comparisons such as
        # "below the average" intentionally do not enter this contract.
        negative_relation = re.search(
            r"(?P<prefix>.+?)\b(?:without(?:\s+any)?|"
            r"(?:who|that|which)\s+(?:do|does)\s+not\s+(?:have|own)(?:\s+any)?|"
            r"(?:who|that|which)\s+(?:has|have|owns?)\s+no|with\s+no)\s+"
            r"(?P<object>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*){0,2})"
            r"(?=\s+\b(?:and|who|that|which|where|whose|lost|located|"
            r"recorded|performed|provided|made|opened|held)\b|\s*[,?.]|$)",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if negative_relation:
            outer_tables = named_tables(negative_relation.group("prefix"))
            inner_tables = [
                name for name in named_tables(negative_relation.group("object"))
                if name not in outer_tables
            ]
            if len(outer_tables) == 1 and inner_tables:
                path_candidates = [
                    (name, unique_fk_path(outer_tables[0], name))
                    for name in inner_tables
                ]
                path_candidates = [item for item in path_candidates if item[1]]
                shortest = min(
                    (len(item[1]["edges"]) for item in path_candidates), default=0,
                )
                nearest = [
                    item for item in path_candidates
                    if len(item[1]["edges"]) == shortest
                ]
                if len(nearest) == 1:
                    inner_table, path = nearest[0]
                    object_roots = lexical_roots(negative_relation.group("object"))
                    terminal_roots = lexical_roots(inner_table)
                    if len(path.get("tables") or []) > 2 \
                            and object_roots.issubset(terminal_roots):
                        # With no terminal-entity predicate, existence of the
                        # first FK bridge row already proves the relationship.
                        # Requiring every downstream edge would reject the
                        # canonical correlated anti-semijoin and add needless
                        # joins.  Qualified objects remain on the full path.
                        inner_table = str(path["tables"][1])
                        path = {
                            "tables": [outer_tables[0], inner_table],
                            "edges": [path["edges"][0]],
                            "source": "unique_shortest_declared_fk_path",
                        }
                    contract.correlation_requirements.append({
                        "kind": "anti_relationship",
                        "quantifier": "not_exists",
                        "outer_table": outer_tables[0],
                        "inner_table": inner_table,
                        "path": path,
                    })
                    require("correlated_anti_join", "explicit_negative_relationship")

        superlative_match = re.search(
            r"\b(?:most|fewest|highest|lowest|top\s+1)\b|"
            r"(?<!\bat\s)\bleast\b|\bfirst\b(?!\s+name\b)|"
            r"最多|最少|最高|最低|第一",
            text,
            re.IGNORECASE,
        )
        has_true_superlative = bool(superlative_match)
        if has_true_superlative:
            explicit_ties = bool(re.search(
                r"\b(?:all\s+ties|including\s+ties|tied)\b|并列",
                text,
                re.IGNORECASE,
            ))
            singular_entity = re.search(
                r"\b(?:of|for)\s+the\s+(?P<entity>[A-Za-z][\w-]*)\b",
                text,
                re.IGNORECASE,
            )
            raw_entity = singular_entity.group("entity") if singular_entity else ""
            explicitly_singular = bool(
                raw_entity
                and not raw_entity.casefold().endswith("s")
            )
            which_subject = re.match(
                r"^\s*which\s+(?P<subject>.+?)\s+"
                r"(?P<aux>is|are|was|were|has|have|does|do|did)\b",
                text,
                re.IGNORECASE | re.DOTALL,
            )
            which_head = (
                re.findall(r"[A-Za-z][\w-]*", which_subject.group("subject"))[-1]
                if which_subject
                and re.findall(r"[A-Za-z][\w-]*", which_subject.group("subject"))
                else ""
            )
            singular_which_predicate = bool(
                which_subject
                and which_subject.group("aux").casefold()
                in {"is", "was", "has", "does", "did"}
                and which_head
                and not which_head.casefold().endswith("s")
            )
            plural_which_predicate = bool(
                which_subject
                and which_head.casefold().endswith("s")
                and which_subject.group("aux").casefold()
                in {"are", "were", "have", "do"}
            )
            initial_which_entity = re.match(
                r"^\s*which\s+(?:the\s+)?(?P<entity>[A-Za-z][\w-]*)\b",
                text,
                re.IGNORECASE,
            )
            singular_which_named_entity = bool(
                initial_which_entity
                and not initial_which_entity.group("entity").casefold().endswith("s")
                and len(named_tables(initial_which_entity.group("entity"))) == 1
            )
            superlative_prefix = text[:superlative_match.start()]
            singular_named_determiner = any(
                not table.name.casefold().endswith("s")
                and re.search(
                    r"\bthe\s+"
                    + r"[\s_-]+".join(
                        re.escape(token)
                        for token in re.findall(r"[A-Za-z0-9]+", table.name)
                    )
                    + r"\b",
                    superlative_prefix,
                    re.IGNORECASE,
                )
                for table in self.schema.tables.values()
                if re.findall(r"[A-Za-z0-9]+", table.name)
            )
            definite_scalar_entity = re.search(
                r"\bthe\s+(?P<entity>[A-Za-z][\w-]*)\s+"
                r"(?:with|having)\s+(?:the\s+)?"
                r"(?:most|least|highest|lowest|largest|smallest)\b",
                text,
                re.IGNORECASE,
            )
            singular_definite_scalar = bool(
                definite_scalar_entity
                and not definite_scalar_entity.group("entity").casefold().endswith("s")
            )
            initial_what = re.match(
                r"^\s*what\s+(?P<aux>is|are)\s+(?:the\s+)?"
                r"(?P<head>[A-Za-z][\w-]*)\b",
                text,
                re.IGNORECASE,
            )
            singular_what_result = bool(
                initial_what and initial_what.group("aux").casefold() == "is"
            )
            plural_what_result = bool(
                initial_what
                and initial_what.group("aux").casefold() == "are"
                and initial_what.group("head").casefold().endswith("s")
                and " and " not in text[:superlative_match.start()].casefold()
            )
            contract.tie_policy = (
                "all_ties" if (
                    explicit_ties or plural_which_predicate or plural_what_result
                )
                else "single_row" if (
                    explicitly_singular or singular_which_predicate
                    or singular_which_named_entity or singular_named_determiner
                    or singular_definite_scalar or singular_what_result
                )
                # Pronouns such as ``who`` and grammatically ambiguous heads do
                # not prove either cardinality.  Leaving the policy undeclared
                # avoids rejecting a valid LIMIT 1 or all-ties formulation and
                # prevents the native planner from owning an underspecified
                # query.
                else ""
            )

        # Spending superlatives are measure rankings, not relationship counts.
        # Bind the amount column only when the wording, numeric schema column,
        # entity key and one declared FK path all agree.  This prevents a model
        # from silently replacing "spent the most" with COUNT(fact rows).
        spend_superlative = re.search(
            r"\b(?P<entity>[A-Za-z][\w-]*)\s+who\s+"
            r"(?:(?:has|have)\s+)?(?:spent|paid)\s+(?:the\s+)?"
            r"(?P<direction>most|least|largest|smallest)\s+"
            r"(?:(?:amount\s+of\s+)?money\s+)?(?:in\s+total\s+)?"
            r"(?:on|for|in)\s+(?:all\s+)?(?P<fact>.+?)(?=[?.]|$)",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if spend_superlative:
            entity_tables = named_tables(spend_superlative.group("entity"))
            named_fact_tables = explicit_named_tables(
                spend_superlative.group("fact")
            )
            measure_tokens = {
                "amount", "cost", "expense", "paid", "payment", "price",
                "spend", "spent", "total",
            }
            measure_candidates: List[tuple[str, DBColumn]] = []
            candidate_tables = (
                [self.schema.tables[name] for name in named_fact_tables]
                if named_fact_tables else list(self.schema.tables.values())
            )
            for table in candidate_tables:
                for column in table.columns:
                    if not re.search(
                        r"(?:INT|REAL|NUM|DEC|FLOAT|DOUBLE)",
                        column.type or "",
                        re.IGNORECASE,
                    ):
                        continue
                    column_tokens = self._normalized_language_tokens(column.name)
                    metadata_tokens = self._normalized_language_tokens(" ".join((
                        column.semantic_name, column.description,
                    )))
                    if (column_tokens | metadata_tokens) & measure_tokens:
                        measure_candidates.append((table.name, column))
            if len(entity_tables) == 1 and len(measure_candidates) == 1:
                entity_table = entity_tables[0]
                measure_table, measure_column = measure_candidates[0]
                path = unique_fk_path(entity_table, measure_table)
                entity_keys = [
                    column for column in self.schema.tables[entity_table].columns
                    if column.pk
                ]
                if path and entity_keys:
                    prefix_tokens = self._normalized_language_tokens(
                        text[:spend_superlative.start()]
                    )
                    output_bindings = [
                        {"table": entity_table, "column": column.name}
                        for column in self.schema.tables[entity_table].columns
                        if self._normalized_language_tokens(column.name)
                        and self._normalized_language_tokens(column.name).issubset(
                            prefix_tokens
                        )
                    ]
                    output_bindings = self._order_output_bindings_by_question(
                        text[:spend_superlative.start()], output_bindings,
                    )
                    if output_bindings and not contract.output_bindings:
                        contract.output_bindings.extend(output_bindings)
                        contract.output_columns.extend(
                            str(item["column"]) for item in output_bindings
                        )
                        require(
                            "exact_output_projection",
                            "spending_superlative_entity_projection",
                        )
                    keys = [
                        f"{entity_table}.{column.name}" for column in entity_keys
                    ]
                    declare_result_grain(
                        kind="entity",
                        owner_table=entity_table,
                        identity_columns=keys,
                        visible_bindings=(output_bindings or contract.output_bindings),
                        cardinality=(contract.tie_policy or "unknown"),
                        source="spending_superlative_entity_identity",
                    )
                    for key in keys:
                        if key not in contract.grouping_keys:
                            contract.grouping_keys.append(key)
                    aggregate_requirement = {
                        "function": "SUM",
                        "column": f"{measure_table}.{measure_column.name}",
                    }
                    if aggregate_requirement not in contract.aggregate_requirements:
                        contract.aggregate_requirements.append(aggregate_requirement)
                    declare_aggregate_subject(
                        role="ranking_measure",
                        function="SUM",
                        source_table=measure_table,
                        column=measure_column.name,
                        multiplicity="measure_values",
                        group_grain=keys,
                        source="explicit_total_measure_subject",
                    )
                    stage_one = {
                        "stage": 1,
                        "kind": "group_aggregate",
                        "group_keys": keys,
                        "aggregates": [{
                            "function": "SUM",
                            "column": measure_column.name,
                            "source_table": measure_table,
                        }],
                        "output_grain": keys,
                    }
                    if stage_one not in contract.aggregation_stages:
                        contract.aggregation_stages.append(stage_one)
                    direction_word = spend_superlative.group("direction").casefold()
                    contract.aggregation_stages.append({
                        "stage": 2,
                        "kind": "rank",
                        "input_stage": 1,
                        "direction": (
                            "DESC" if direction_word in {"most", "largest"}
                            else "ASC"
                        ),
                        "limit": 1 if contract.tie_policy == "single_row" else None,
                    })
                    if path not in contract.relation_paths:
                        contract.relation_paths.append(path)
                    require("aggregate_input", "spending_amount_measure")
                    require("entity_reduction", "spending_entity_grain")
                    require("aggregation_stage", "sum_before_spending_rank")

        # Counted relationship superlatives appear in several natural forms
        # that the older "the <entity> <relation> most" pattern cannot cover:
        # "which conductor ... most number of orchestras", "contestant ...
        # least votes", etc.  Compile the two-stage algebra only when the text
        # names exactly one entity table and one fact table and the schema proves
        # a unique FK path.  Direct measure superlatives ("lowest earnings") and
        # spend/sum wording ("spent the most on treatments") remain untouched.
        counted_superlative = self._COUNTED_RELATIONSHIP_SUPERLATIVE_RE.search(text)
        if counted_superlative and not spend_superlative:
            # The entity prefix commonly says "stadium name" or "series id";
            # there the table noun legitimately also qualifies an output
            # column, so the counted-relationship grammar provides the extra
            # context needed to use bounded fuzzy table routing.
            entity_tables = named_tables(text[:counted_superlative.start()])
            # The counted noun is sometimes a business label rather than the
            # physical fact table ("most awards in evaluations").  Search the
            # bounded remainder of the question so an explicitly named fact
            # table can supply the unique FK-backed relationship.
            relation_tables = named_tables(text[counted_superlative.start():])
            relation_tables = [name for name in relation_tables if name not in entity_tables]
            if len(entity_tables) == 1 and len(relation_tables) == 1:
                entity_table = entity_tables[0]
                fact_table = relation_tables[0]
                path = unique_fk_path(entity_table, fact_table)
                entity_keys = [
                    column for column in self.schema.tables[entity_table].columns
                    if column.pk
                ]
                if path and entity_keys:
                    projection_prefix = text[:counted_superlative.start()]
                    projection_phrase = re.split(
                        r"\b(?:that|who|with|has|have|had)\b|\bis\s+used\s+by\b",
                        projection_prefix,
                        maxsplit=1,
                        flags=re.IGNORECASE,
                    )[0]
                    projection_bindings = exact_projection_bindings(
                        projection_phrase, entity_table,
                    )
                    if not projection_bindings and re.search(
                        r"\b(?:which|what)\s+(?:the\s+)?[A-Za-z][\w-]*\b|"
                        r"\bthe\s+[A-Za-z][\w-]*\b",
                        projection_phrase,
                        re.IGNORECASE,
                    ):
                        name_columns = [
                            column for column in self.schema.tables[entity_table].columns
                            if "name" in self._normalized_language_tokens(column.name)
                        ]
                        if len(name_columns) == 1:
                            projection_bindings = [{
                                "table": entity_table,
                                "column": name_columns[0].name,
                            }]
                    if projection_bindings and not contract.output_bindings:
                        contract.output_bindings.extend(projection_bindings)
                        for binding in projection_bindings:
                            column_name = str(binding["column"])
                            if column_name not in contract.output_columns:
                                contract.output_columns.append(column_name)
                        require(
                            "exact_output_projection",
                            "counted_relationship_physical_projection",
                        )
                    prefix_tokens = self._normalized_language_tokens(
                        text[:counted_superlative.start()]
                    )
                    entity_tokens = self._normalized_language_tokens(entity_table)
                    explicit_grain_columns = []
                    for column in self.schema.tables[entity_table].columns:
                        column_tokens = self._normalized_language_tokens(column.name)
                        if (
                            not column.pk
                            and len(column_tokens) >= 2
                            and bool(column_tokens & entity_tokens)
                            and column_tokens.issubset(prefix_tokens)
                        ):
                            explicit_grain_columns.append(column)
                    category_value_subject = bool(
                        len(projection_bindings) == 1
                        and len(explicit_grain_columns) == 1
                        and str(projection_bindings[0].get("column") or "").casefold()
                        == explicit_grain_columns[0].name.casefold()
                        and re.search(
                            r"\b(?:is|are|was|were)\s+"
                            r"(?:used|seen|found|selected|chosen)\s+by\b",
                            projection_prefix,
                            re.IGNORECASE,
                        )
                    )
                    # The entity primary key owns row identity.  A requested
                    # label or name remains a visible column and must not
                    # replace that key.  The only bounded exception is an
                    # explicit categorical value subject ("which template
                    # type code is used by ..."), whose value itself is the
                    # requested grouping grain rather than an entity display.
                    grain_columns = (
                        explicit_grain_columns
                        if category_value_subject else entity_keys
                    )
                    keys = [f"{entity_table}.{column.name}" for column in grain_columns]
                    declare_result_grain(
                        kind=("group_value" if category_value_subject else "entity"),
                        owner_table=entity_table,
                        identity_columns=keys,
                        visible_bindings=(projection_bindings or contract.output_bindings),
                        cardinality=(contract.tie_policy or "unknown"),
                        multiplicity=(
                            "one_row_per_group_value" if category_value_subject
                            else "one_row_per_entity"
                        ),
                        source=(
                            "explicit_category_value_identity"
                            if category_value_subject
                            else "counted_relationship_entity_identity"
                        ),
                    )
                    declare_aggregate_subject(
                        role="ranking_measure",
                        function="COUNT",
                        source_table=fact_table,
                        column="*",
                        multiplicity="fact_rows",
                        group_grain=keys,
                        source="counted_relationship_fact_rows",
                    )
                    for key in keys:
                        if key not in contract.grouping_keys:
                            contract.grouping_keys.append(key)
                    stage_one = {
                        "stage": 1,
                        "kind": "group_aggregate",
                        "group_keys": keys,
                        "aggregates": [{
                            "function": "COUNT",
                            "column": "*",
                            "source_table": fact_table,
                        }],
                        "output_grain": keys,
                    }
                    if stage_one not in contract.aggregation_stages:
                        contract.aggregation_stages.append(stage_one)
                    contract.aggregation_stages.append({
                        "stage": 2,
                        "kind": "rank",
                        "input_stage": 1,
                        "direction": (
                            "DESC" if counted_superlative.group("direction").casefold()
                            == "most" else "ASC"
                        ),
                        "limit": 1 if contract.tie_policy == "single_row" else None,
                    })
                    if path not in contract.relation_paths:
                        contract.relation_paths.append(path)
                    require("entity_reduction", "counted_relationship_entity_grain")
                    require("aggregation_stage", "aggregate_before_superlative_rank")

        # A coordinated request can return both an entity label and the number
        # of related records.  This is not a scalar ``how many`` request and it
        # is not a ranking: the aggregate is itself a requested output at the
        # entity grain.  Compile only the closed grammatical shape, exact
        # physical label, unique entity/fact tables and unique declared FK path.
        grouped_relationship_count = self._GROUPED_RELATIONSHIP_COUNT_RE.match(text)
        if grouped_relationship_count and not contract.output_layout:
            entity_phrase = grouped_relationship_count.group("entity")
            fact_phrase = grouped_relationship_count.group("fact")
            entity_tables = named_tables(entity_phrase)
            fact_tables = [
                name for name in named_tables(fact_phrase)
                if name not in entity_tables
            ]
            if len(entity_tables) == 1 and len(fact_tables) == 1:
                entity_table = entity_tables[0]
                fact_table = fact_tables[0]
                path = unique_fk_path(entity_table, fact_table)
                entity_keys = [
                    column for column in self.schema.tables[entity_table].columns
                    if column.pk
                ]
                label_phrase = (
                    entity_phrase + " "
                    + grouped_relationship_count.group("label")
                )
                projection_bindings = exact_projection_bindings(
                    label_phrase, entity_table,
                )
                if path and entity_keys and len(projection_bindings) == 1:
                    binding = projection_bindings[0]
                    contract.output_bindings.append(binding)
                    contract.output_columns.append(str(binding["column"]))
                    fact_words = re.findall(r"[A-Za-z0-9]+", fact_phrase)
                    fact_root = fact_words[-1].casefold() if fact_words else "related"
                    if len(fact_root) > 2 and fact_root.endswith("s") \
                            and not fact_root.endswith(("ss", "us", "is")):
                        fact_root = fact_root[:-1]
                    alias = re.sub(r"[^A-Za-z0-9_]+", "_", fact_root).strip("_")
                    aggregate_output = {
                        "kind": "aggregate",
                        "function": "COUNT",
                        "source_table": fact_table,
                        "column": "*",
                        "alias": (alias or "related") + "_count",
                    }
                    contract.output_layout.extend([
                        {
                            "kind": "column",
                            "table": entity_table,
                            "column": str(binding["column"]),
                        },
                        aggregate_output,
                    ])
                    keys = [
                        f"{entity_table}.{column.name}" for column in entity_keys
                    ]
                    declare_result_grain(
                        kind="entity",
                        owner_table=entity_table,
                        identity_columns=keys,
                        visible_bindings=[binding],
                        cardinality="set",
                        source="grouped_relationship_entity_identity",
                    )
                    declare_aggregate_subject(
                        role="visible_measure",
                        function="COUNT",
                        source_table=fact_table,
                        column="*",
                        multiplicity="fact_rows",
                        group_grain=keys,
                        source="grouped_relationship_fact_rows",
                    )
                    contract.grouping_keys.extend(keys)
                    contract.aggregation_stages.append({
                        "stage": 1,
                        "kind": "group_aggregate",
                        "group_keys": keys,
                        "aggregates": [{
                            "function": "COUNT",
                            "column": "*",
                            "source_table": fact_table,
                            "output": True,
                            "alias": aggregate_output["alias"],
                        }],
                        "output_grain": keys,
                        "include_zero": True,
                    })
                    contract.relation_paths.append(path)
                    require(
                        "exact_output_projection",
                        "grouped_relationship_count_physical_output",
                    )
                    require("aggregate_output", "relationship_count_is_requested_output")
                    require("group_by", "grouped_relationship_count_entity_grain")
                    require("grouping_entity_key", "declared_entity_primary_key")
                    require("aggregation_stage", "group_before_relationship_count_output")
                    require("relation_path", "unique_declared_fk_path")

        if contract.grouping_keys and contract.aggregate_requirements and not any(
            stage.get("kind") == "group_aggregate"
            for stage in contract.aggregation_stages
        ):
            contract.aggregation_stages.append({
                "stage": 1,
                "kind": "group_aggregate",
                "group_keys": list(contract.grouping_keys),
                "aggregates": [dict(item) for item in contract.aggregate_requirements],
                "output_grain": list(contract.grouping_keys),
            })
            require("aggregation_stage", "group_before_measure")

        # A categorical usage superlative groups by the requested category
        # value, not by an arbitrary row/entity primary key.  Example shape:
        # "the code of the template type most commonly used in documents".
        # Duplicate category columns are resolved only by a unique shortest FK
        # distance to the explicitly named fact table.
        category_usage_compiled = False
        category_usage = re.search(
            r"\b(?P<label>codes?|names?|ids?|identifiers?)\s+of\s+"
            r"(?:the\s+)?(?P<category>[A-Za-z][\w-]*"
            r"(?:\s+[A-Za-z][\w-]*){0,2}?)\s+"
            r"(?:that|which)\s+(?:is|are|was|were)\s+"
            r"(?P<direction>most|least)\s+"
            r"(?:(?:commonly|frequently|often)\s+)?"
            r"(?:used|seen|found|selected|chosen)\s+"
            r"(?:in|by|across)\s+(?:the\s+)?"
            r"(?P<fact>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*){0,2})"
            r"(?=[?.]|$)",
            text,
            re.IGNORECASE,
        )
        if category_usage:
            fact_tables = named_tables(category_usage.group("fact"))
            category_refs = phrase_column_refs(
                category_usage.group("category") + " "
                + category_usage.group("label")
            )
            nearest = nearest_column_binding(category_refs, fact_tables)
            if len(fact_tables) == 1 and nearest is not None:
                (category_table, category_column), _distance_path = nearest
                fact_table = fact_tables[0]
                path = unique_fk_path(category_table, fact_table)
                if path is not None and category_table != fact_table:
                    binding = {
                        "table": category_table, "column": category_column,
                    }
                    if not contract.output_bindings:
                        contract.output_bindings.append(binding)
                        contract.output_columns.append(category_column)
                    key = f"{category_table}.{category_column}"
                    declare_result_grain(
                        kind="group_value",
                        owner_table=category_table,
                        identity_columns=[key],
                        visible_bindings=[binding],
                        cardinality=(contract.tie_policy or "unknown"),
                        multiplicity="one_row_per_group_value",
                        source="category_usage_value_identity",
                    )
                    declare_aggregate_subject(
                        role="ranking_measure",
                        function="COUNT",
                        source_table=fact_table,
                        column="*",
                        multiplicity="fact_rows",
                        group_grain=[key],
                        source="category_usage_fact_rows",
                    )
                    if not contract.grouping_keys:
                        contract.grouping_keys.append(key)
                    if not contract.aggregation_stages:
                        contract.aggregation_stages.extend([
                            {
                                "stage": 1,
                                "kind": "group_aggregate",
                                "group_keys": [key],
                                "aggregates": [{
                                    "function": "COUNT",
                                    "column": "*",
                                    "source_table": fact_table,
                                }],
                                "output_grain": [key],
                            },
                            {
                                "stage": 2,
                                "kind": "rank",
                                "input_stage": 1,
                                "direction": (
                                    "DESC" if category_usage.group("direction")
                                    .casefold() == "most" else "ASC"
                                ),
                                "limit": (
                                    1 if contract.tie_policy == "single_row" else None
                                ),
                            },
                        ])
                    if path not in contract.relation_paths:
                        contract.relation_paths.append(path)
                    require(
                        "exact_output_projection",
                        "category_usage_physical_projection",
                    )
                    require("entity_reduction", "category_usage_value_grain")
                    require("aggregation_stage", "aggregate_before_category_rank")
                    category_usage_compiled = True

        # Relationship superlatives have two ordered operations: reduce facts
        # to one measure per entity, then rank those entity rows. Do not infer
        # this shape unless the entity table, fact table and FK path are unique.
        relationship_superlative = re.search(
            r"\bthe\s+(?P<entity>[A-Za-z][\w-]*)\s+"
            r"(?P<relation>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*){0,2}?)\s+"
            r"(?P<direction>most|least)\b",
            text,
            re.IGNORECASE,
        )
        relation_phrase_has_clause_filler = bool(
            relationship_superlative
            and lexical_roots(relationship_superlative.group("relation"))
            & {"are", "is", "that", "which", "who", "was", "were"}
        )
        if relationship_superlative and not category_usage_compiled \
                and not relation_phrase_has_clause_filler:
            entity_tables = named_tables(relationship_superlative.group("entity"))
            relation_tables = [
                name for name in named_tables(relationship_superlative.group("relation"))
                if name not in entity_tables
            ]
            if len(entity_tables) == 1 and len(relation_tables) == 1:
                path = unique_fk_path(entity_tables[0], relation_tables[0])
                entity_keys = [
                    column for column in self.schema.tables[entity_tables[0]].columns
                    if column.pk
                ]
                if path and entity_keys:
                    keys = [f"{entity_tables[0]}.{column.name}" for column in entity_keys]
                    declare_result_grain(
                        kind="entity",
                        owner_table=entity_tables[0],
                        identity_columns=keys,
                        visible_bindings=[
                            item for item in contract.output_bindings
                            if str(item.get("table") or "") == entity_tables[0]
                        ],
                        cardinality=(contract.tie_policy or "unknown"),
                        source="relationship_superlative_entity_identity",
                    )
                    declare_aggregate_subject(
                        role="ranking_measure",
                        function="COUNT",
                        source_table=relation_tables[0],
                        column="*",
                        multiplicity="fact_rows",
                        group_grain=keys,
                        source="relationship_superlative_fact_rows",
                    )
                    for key in keys:
                        if key not in contract.grouping_keys:
                            contract.grouping_keys.append(key)
                    stage_one = {
                        "stage": 1,
                        "kind": "group_aggregate",
                        "group_keys": keys,
                        "aggregates": [{
                            "function": "COUNT",
                            "column": "*",
                            "source_table": relation_tables[0],
                        }],
                        "output_grain": keys,
                    }
                    if stage_one not in contract.aggregation_stages:
                        contract.aggregation_stages.append(stage_one)
                    contract.aggregation_stages.append({
                        "stage": 2,
                        "kind": "rank",
                        "input_stage": 1,
                        "direction": (
                            "DESC" if relationship_superlative.group("direction").casefold()
                            == "most" else "ASC"
                        ),
                        "limit": 1 if contract.tie_policy == "single_row" else None,
                    })
                    if path not in contract.relation_paths:
                        contract.relation_paths.append(path)
                    require("entity_reduction", "relationship_superlative_entity_grain")
                    require("aggregation_stage", "aggregate_before_superlative_rank")

        # Bind explicit year boundaries as typed scalar predicates.  The
        # column must be unique within the already proven relational path;
        # otherwise the request stays in the model/clarification path.
        year_boundary = re.search(
            r"\b(?:in\s+)?year\s+(?P<year>\d{4})\s+"
            r"(?P<direction>or\s+after|and\s+later|or\s+later|or\s+before|and\s+earlier|or\s+earlier)\b",
            text,
            re.IGNORECASE,
        )
        if year_boundary:
            path_tables = list(dict.fromkeys(
                table_name
                for path in contract.relation_paths
                if path.get("source") == "unique_shortest_declared_fk_path"
                for table_name in path.get("tables") or []
            ))
            search_tables = path_tables or list(self.schema.tables)
            year_columns = [
                (table_name, column.name)
                for table_name in search_tables
                for column in self.schema.tables[table_name].columns
                if self._normalized_language_tokens(column.name) == {"year"}
            ]
            if len(year_columns) == 1:
                direction = year_boundary.group("direction").casefold()
                requirement = {
                    "column": f"{year_columns[0][0]}.{year_columns[0][1]}",
                    "operator": "<=" if direction.endswith(("before", "earlier")) else ">=",
                    "value": int(year_boundary.group("year")),
                    "value_type": "number",
                    "scope": "row_predicate",
                }
                if requirement not in contract.filter_requirements:
                    contract.filter_requirements.append(requirement)
                require("typed_filter", "explicit_year_boundary")

        # Explicit independent set semantics are not a JOIN request.  Compile
        # only when both entity phrases resolve to one table and the requested
        # physical output column exists uniquely on both branches.
        set_match = re.search(
            r"(?P<output>.+?)\bwhere\s+both\s+"
            r"(?P<left>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*)?)\s+and\s+"
            r"(?P<right>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*)?)\s+"
            r"(?:live|work|reside|occur|appear)\b",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if set_match:
            left_tables = named_tables(set_match.group("left"))
            right_tables = named_tables(set_match.group("right"))
            output_set_tokens = self._normalized_language_tokens(set_match.group("output"))

            def set_output(table_name: str) -> List[str]:
                return [
                    column.name for column in self.schema.tables[table_name].columns
                    if self._normalized_language_tokens(column.name)
                    and self._normalized_language_tokens(column.name).issubset(output_set_tokens)
                ]

            if len(left_tables) == 1 and len(right_tables) == 1 \
                    and left_tables[0] != right_tables[0]:
                left_columns = set_output(left_tables[0])
                right_columns = set_output(right_tables[0])
                if len(left_columns) == 1 and len(right_columns) == 1 \
                        and self._normalized_language_tokens(left_columns[0]) \
                        == self._normalized_language_tokens(right_columns[0]):
                    contract.set_requirements.append({
                        "operator": "INTERSECT",
                        "branches": [
                            {"table": left_tables[0], "column": left_columns[0]},
                            {"table": right_tables[0], "column": right_columns[0]},
                        ],
                        "output_name": left_columns[0],
                        "row_grain": "set_of_values",
                    })
                    require("set_intersection", "explicit_both_entity_sets")

        # A closed anti-set request over a declared FK is a set difference, not
        # an inferred JOIN.  Compile only the entity key and the matching fact
        # FK; descriptive outputs would require an additional relational stage
        # and therefore stay outside this bounded native plan.
        difference_match = re.search(
            r"\b(?P<label>ids?|identifiers?|codes?)\s+(?:for|of)\s+"
            r"(?:all\s+)?(?P<entity>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*)?)\s+"
            r"(?:that\s+(?:are\s+)?)?(?:not|never)\s+"
            r"(?:used|referenced|linked|associated)\s+by\s+(?:any\s+)?"
            r"(?P<fact>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*)?)"
            r"(?=[?.]|$)",
            text,
            re.IGNORECASE,
        )
        if difference_match and not contract.set_requirements:
            entity_tables = named_tables(difference_match.group("entity"))
            fact_tables = named_tables(difference_match.group("fact"))
            if len(entity_tables) == 1 and len(fact_tables) == 1 \
                    and entity_tables[0] != fact_tables[0]:
                entity_table = self.schema.tables[entity_tables[0]]
                fact_table = self.schema.tables[fact_tables[0]]
                entity_keys = [column for column in entity_table.columns if column.pk]
                fact_keys = [
                    column for column in fact_table.columns
                    if column.fk_table == entity_table.name and column.fk_column
                    and any(
                        key.name.casefold() == column.fk_column.casefold()
                        for key in entity_keys
                    )
                ]
                if len(entity_keys) == 1 and len(fact_keys) == 1:
                    entity_key = entity_keys[0]
                    fact_key = fact_keys[0]
                    contract.set_requirements.append({
                        "operator": "EXCEPT",
                        "branches": [
                            {"table": entity_table.name, "column": entity_key.name},
                            {"table": fact_table.name, "column": fact_key.name},
                        ],
                        "output_name": entity_key.name,
                        "row_grain": "set_of_values",
                        "proof_edge": {
                            "from": f"{fact_table.name}.{fact_key.name}",
                            "to": f"{entity_table.name}.{entity_key.name}",
                            "source": "foreign_key",
                        },
                    })
                    contract.output_columns.append(entity_key.name)
                    contract.output_bindings.append({
                        "table": entity_table.name, "column": entity_key.name,
                    })
                    require("set_difference", "explicit_fk_anti_set")

        distinct_match = re.search(
            r"\b(?:how\s+many|count(?:\s+the)?|number\s+of)\s+"
            r"(?:different|distinct|unique)\s+"
            r"(?P<entity>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*)?)\s+"
            r"(?:have|has|with|used\s+in|appearing\s+in)\s+"
            r"(?P<fact>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*)?)",
            text,
            re.IGNORECASE,
        )
        if distinct_match:
            entity_tables = named_tables(distinct_match.group("entity"))
            fact_tables = [
                name for name in named_tables(distinct_match.group("fact"))
                if name not in entity_tables
            ]
            if len(entity_tables) == 1 and len(fact_tables) == 1:
                entity_table = entity_tables[0]
                fact_table = fact_tables[0]
                entity_keys = [
                    column for column in self.schema.tables[entity_table].columns if column.pk
                ]
                fact_keys = [
                    column for column in self.schema.tables[fact_table].columns
                    if column.fk_table == entity_table and column.fk_column
                    and any(
                        key.name.casefold() == column.fk_column.casefold()
                        for key in entity_keys
                    )
                ]
                if len(entity_keys) == 1 and len(fact_keys) == 1:
                    contract.distinct_count_requirements.append({
                        "source_table": fact_table,
                        "column": f"{fact_table}.{fact_keys[0].name}",
                        "entity_table": entity_table,
                        "entity_key": f"{entity_table}.{entity_keys[0].name}",
                        "proof_edge": {
                            "from": f"{fact_table}.{fact_keys[0].name}",
                            "to": f"{entity_table}.{entity_keys[0].name}",
                            "source": "foreign_key",
                        },
                    })
                    declare_aggregate_subject(
                        role="visible_measure",
                        function="COUNT",
                        source_table=fact_table,
                        column=fact_keys[0].name,
                        multiplicity="distinct_entities",
                        group_grain=[],
                        source="explicit_distinct_entity_subject",
                    )
                    require("distinct_entity_count", "declared_fk_distinct_entity_count")

        # Counting entities that "have" related facts is distinct by entity
        # even when the user does not say the implementation word DISTINCT.
        # Bind this only when one entity PK and one incoming FK-backed fact
        # source remain after optional object-table grounding.
        implicit_entity_count = re.search(
            r"\b(?:how\s+many|number\s+of)\s+"
            r"(?P<entity>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*)?)\s+"
            r"(?:who|that)\s+(?:have|has)\s+(?:ever\s+)?"
            r"(?P<relation>.+?)(?=[?.]|$)",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if implicit_entity_count and not contract.distinct_count_requirements:
            entity_tables = named_tables(implicit_entity_count.group("entity"))
            if len(entity_tables) == 1:
                entity_table = entity_tables[0]
                entity_keys = [
                    column for column in self.schema.tables[entity_table].columns
                    if column.pk
                ]
                incoming: List[tuple[str, DBColumn]] = []
                if len(entity_keys) == 1:
                    entity_key = entity_keys[0]
                    for table in self.schema.tables.values():
                        for column in table.columns:
                            if column.fk_table == entity_table and column.fk_column \
                                    and column.fk_column.casefold() == entity_key.name.casefold():
                                incoming.append((table.name, column))
                    object_tables = [
                        name for name in named_tables(
                            implicit_entity_count.group("relation")
                        )
                        if name != entity_table
                    ]
                    if object_tables:
                        grounded = [
                            item for item in incoming
                            if any(
                                item[0] == object_table
                                or unique_fk_path(item[0], object_table) is not None
                                for object_table in object_tables
                            )
                        ]
                        if grounded:
                            incoming = grounded
                    unique_facts = {
                        (table_name, column.name): column
                        for table_name, column in incoming
                    }
                    if len(unique_facts) == 1:
                        (fact_table, fact_column), _column = next(iter(unique_facts.items()))
                        contract.distinct_count_requirements.append({
                            "source_table": fact_table,
                            "column": f"{fact_table}.{fact_column}",
                            "entity_table": entity_table,
                            "entity_key": f"{entity_table}.{entity_key.name}",
                            "proof_edge": {
                                "from": f"{fact_table}.{fact_column}",
                                "to": f"{entity_table}.{entity_key.name}",
                                "source": "foreign_key",
                            },
                        })
                        declare_aggregate_subject(
                            role="visible_measure",
                            function="COUNT",
                            source_table=fact_table,
                            column=fact_column,
                            multiplicity="distinct_entities",
                            group_grain=[],
                            source="implicit_distinct_entity_subject",
                        )
                        require(
                            "distinct_entity_count",
                            "implicit_entity_count_over_declared_fk_facts",
                        )

        # Promote a closed, schema-bound output phrase into the relational
        # contract even when the query is not one of the native aggregate/set
        # slices above.  This lets the projection lock repair harmless extra
        # SELECT expressions and gives bounded candidate search enough
        # independent evidence to compare semantic alternatives.  Exact
        # physical-token bindings are preferred; a fuzzy single-column binding
        # is allowed only for the narrow "order by count" modifier form, where
        # the aggregate is explicitly a sort key rather than a requested value.
        scalar_attribute_how_many = bool(re.search(
            r"^\s*how\s+many\s+[A-Za-z][\w-]*"
            r"(?:\s+[A-Za-z][\w-]*){0,2}?\s+"
            r"(?:does|do|did|has|have|had)\b",
            text,
            re.IGNORECASE,
        ))
        count_how_many = bool(re.match(
            r"^\s*how\s+many\b", text, re.IGNORECASE,
        )) and not scalar_attribute_how_many
        if not contract.output_bindings and not count_how_many:
            global_projection_candidates = [
                bindings
                for table_name in self.schema.tables
                for bindings in [exact_projection_bindings(output_phrase, table_name)]
                if bindings
            ]
            mentioned_tables = named_tables(text)
            projection_candidates = [
                bindings
                for table_name in mentioned_tables
                for bindings in [exact_projection_bindings(output_phrase, table_name)]
                if bindings
            ]
            resolved_projection: List[dict] = []
            if len(global_projection_candidates) == 1:
                resolved_projection = global_projection_candidates[0]
            elif len(projection_candidates) == 1:
                resolved_projection = projection_candidates[0]
            elif re.search(
                r"\b(?:ascending|descending)\s+order\s+of\s+(?:the\s+)?count\b|"
                r"\border(?:ed)?\s+by\s+(?:the\s+)?count\b",
                text,
                re.IGNORECASE,
            ):
                refs = phrase_column_refs(output_phrase)
                if len(refs) == 1 and refs[0][0] in mentioned_tables:
                    resolved_projection = [{
                        "table": refs[0][0], "column": refs[0][1],
                    }]
            if resolved_projection:
                contract.output_bindings.extend(resolved_projection)
                for binding in resolved_projection:
                    column_name = str(binding["column"])
                    if column_name not in contract.output_columns:
                        contract.output_columns.append(column_name)
                require("exact_output_projection", "schema_bound_output_phrase")

        # Definite entity output plus a relationship-count threshold is a
        # grouped fact predicate even when the business noun is not the
        # physical bridge-table name (for example, "a transcript with at least
        # 2 course results" recorded in ``Transcript_Contents``).  The bridge
        # is accepted only when the output resolves to one entity with one
        # primary-key shape and one uniquely compatible incoming FK edge.
        direct_relationship_threshold = re.search(
            r"\b(?:with|having)\s+(?P<operator>at\s+least|more\s+than|"
            r"fewer\s+than|less\s+than|exactly)\s+"
            r"(?P<value>\d+|one|two|three|four|five|six|seven|eight|nine|ten)"
            r"\s+(?P<subject>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*){0,2}?)"
            r"(?=[?.。]|$)",
            text,
            re.IGNORECASE,
        )
        direct_threshold_scalar_binding = (
            scalar_threshold_binding(direct_relationship_threshold.group("subject"))
            if direct_relationship_threshold else None
        )
        if direct_relationship_threshold and contract.output_bindings \
                and not contract.relationship_thresholds \
                and direct_threshold_scalar_binding is None:
            entity_tables = list(dict.fromkeys(
                str(item.get("table") or "") for item in contract.output_bindings
                if item.get("table")
            ))
            if len(entity_tables) == 1:
                entity_table = entity_tables[0]
                entity_keys = [
                    column for column in self.schema.tables[entity_table].columns
                    if column.pk
                ]
                incoming: List[tuple[str, DBColumn, dict]] = []
                for table in self.schema.tables.values():
                    for column in table.columns:
                        for entity_key in entity_keys:
                            if column.fk_table == entity_table and column.fk_column \
                                    and column.fk_column.casefold() \
                                    == entity_key.name.casefold():
                                incoming.append((table.name, column, {
                                    "from": f"{table.name}.{column.name}",
                                    "to": f"{entity_table}.{entity_key.name}",
                                    "source": "foreign_key",
                                }))
                subject_tables = named_tables(
                    direct_relationship_threshold.group("subject")
                )
                compatible = []
                for fact_table, fact_column, edge in incoming:
                    if not subject_tables or any(
                        subject_table == fact_table
                        or (
                            (path := unique_fk_path(fact_table, subject_table))
                            is not None
                        )
                        for subject_table in subject_tables
                    ):
                        compatible.append((fact_table, fact_column, edge))
                if len(entity_keys) >= 1 and len(compatible) == 1:
                    fact_table, fact_column, proof_edge = compatible[0]
                    raw_value = direct_relationship_threshold.group("value").casefold()
                    threshold_value = (
                        int(raw_value) if raw_value.isdigit()
                        else number_words[raw_value]
                    )
                    operator = operator_by_phrase[re.sub(
                        r"\s+", " ",
                        direct_relationship_threshold.group("operator").casefold(),
                    )]
                    requirement = {
                        "operator": operator,
                        "value": threshold_value,
                        "subject": direct_relationship_threshold.group("subject"),
                        "entity_table": entity_table,
                        "fact_table": fact_table,
                        "proof_edge": proof_edge,
                    }
                    contract.relationship_thresholds.append(requirement)
                    keys = [
                        f"{entity_table}.{column.name}" for column in entity_keys
                    ]
                    for key in keys:
                        if key not in contract.grouping_keys:
                            contract.grouping_keys.append(key)
                    stage = {
                        "stage": 1,
                        "kind": "group_aggregate",
                        "group_keys": keys,
                        "aggregates": [{
                            "function": "COUNT",
                            "column": "*",
                            "source_table": fact_table,
                        }],
                        "output_grain": keys,
                    }
                    if stage not in contract.aggregation_stages:
                        contract.aggregation_stages.append(stage)
                    path = {
                        "tables": [entity_table, fact_table],
                        "edges": [proof_edge],
                        "source": "unique_incoming_declared_fk_threshold_path",
                    }
                    if path not in contract.relation_paths:
                        contract.relation_paths.append(path)
                    require("group_by", "relationship_count_threshold")
                    require("having", "relationship_count_threshold")
                    require("entity_reduction", "relationship_threshold_entity_grain")
                    require("aggregation_stage", "group_before_relationship_threshold")
                    require("relation_path", "unique_declared_fk_path")

        # Bind "for a <value> <physical label>" as a typed equality only when
        # the output owner gives one uniquely nearest declared-FK column.  This
        # prevents a neighboring attribute (for example Make instead of Model)
        # from receiving the literal merely because both are text columns.
        labeled_literal = re.search(
            r"\b(?:for|with|in)\s+(?:a|an|the)\s+"
            r"(?P<value>[A-Za-z][\w'-]*)\s+"
            r"(?P<label>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*)?)"
            r"(?=\s*[,;?]|\s+\b(?:does|do|did|has|have|had|is|are|"
            r"where|whose|that|which|with|having)\b)",
            text,
            re.IGNORECASE,
        )
        if labeled_literal and contract.output_bindings:
            literal = labeled_literal.group("value")
            generic_values = {
                "any", "certain", "each", "every", "given", "new", "old",
                "particular", "specific", "the", "this",
            }
            anchor_tables = list(dict.fromkeys(
                str(item.get("table") or "") for item in contract.output_bindings
                if item.get("table")
            ))
            label_refs = phrase_column_refs(labeled_literal.group("label"))
            nearest = nearest_column_binding(label_refs, anchor_tables)
            if literal.casefold() not in generic_values and nearest is not None:
                (table_name, column_name), _path = nearest
                column = next(
                    item for item in self.schema.tables[table_name].columns
                    if item.name == column_name
                )
                if not re.search(
                    r"(?:INT|REAL|NUM|DEC|FLOAT|DOUBLE|DATE|TIME|BOOL)",
                    column.type or "",
                    re.IGNORECASE,
                ):
                    requirement = {
                        "column": f"{table_name}.{column_name}",
                        "operator": "=",
                        "value": literal,
                        "value_type": "text",
                        "scope": "row_predicate",
                    }
                    if requirement not in contract.filter_requirements:
                        contract.filter_requirements.append(requirement)
                    require("typed_filter", "schema_labeled_literal")

        # One explicitly quoted value that occurs in exactly one sampled
        # physical column is a closed equality predicate. This also closes a
        # qualified anti-relationship such as "no shipments recorded in
        # 'Region A'" before candidate search. Multiple quoted values remain
        # with the set/Boolean compiler because IN/UNION may be valid.
        quoted_matches = [
            match for match in re.finditer(
                r"(?<![A-Za-z0-9])(?P<quote>['\"])(?P<value>.*?)"
                r"(?P=quote)(?![A-Za-z0-9])",
                text,
                re.DOTALL,
            )
            if match.group("value").strip()
        ]
        fuzzy_literal_request = self._question_requests_fuzzy_matching(text)
        negated_quoted_literal = bool(
            len(quoted_matches) == 1
            and re.search(
                r"\b(?:not|except|excluding|isn't|aren't)\b"
                r"(?:\s+[A-Za-z][\w-]*){0,4}\s*$|"
                r"\bother\s+than(?:\s+[A-Za-z][\w-]*){0,3}\s*$|"
                r"不是|不等于|排除",
                text[max(0, quoted_matches[0].start() - 40):quoted_matches[0].start()],
                re.IGNORECASE,
            )
        )
        if len(quoted_matches) == 1 \
                and not fuzzy_literal_request and not negated_quoted_literal:
            requested_literal = re.sub(
                r"\s+", " ", quoted_matches[0].group("value")
            ).strip()
            grounded: List[tuple[str, str, str]] = []
            for table in self.schema.tables.values():
                for column in table.columns:
                    canonical = {
                        str(value).strip()
                        for value in column.sample_values
                        if value is not None
                        and str(value).strip().casefold()
                        == requested_literal.casefold()
                    }
                    if len(canonical) == 1:
                        grounded.append((
                            table.name, column.name, next(iter(canonical)),
                        ))
            if len(grounded) == 1:
                table_name, column_name, canonical_value = grounded[0]
                requirement = {
                    "column": f"{table_name}.{column_name}",
                    "operator": "=",
                    "value": canonical_value,
                    "value_type": "text",
                    "scope": "row_predicate",
                }
                if requirement not in contract.filter_requirements:
                    contract.filter_requirements.append(requirement)
                literal_policy = {
                    "mode": "question_grounded_string_predicates",
                    "allowed_values": [canonical_value],
                    "value_column": f"{table_name}.{column_name}",
                }
                if literal_policy not in contract.predicate_literal_policies:
                    contract.predicate_literal_policies.append(literal_policy)
                require("typed_filter", "unique_sample_grounded_quoted_literal")
                require(
                    "predicate_literal_provenance",
                    "quoted_literal_closes_candidate_predicate",
                )

        # "each entity's label and description of the related kind" asks for
        # unique descriptive tuples, not one row per repeated fact event.  The
        # requirement is emitted only when both output columns and their unique
        # declared FK path can be bound locally.
        descriptive_tuple = re.search(
            r"^\s*(?:what\s+are|list|show)\s+each\s+"
            r"(?P<entity>[A-Za-z][\w-]*)(?:'s|’s)\s+"
            r"(?P<left>.+?)\s+and\s+(?P<right>.+?)\s+of\s+(?:the\s+)?"
            r"(?P<related>[A-Za-z][\w-]*)\s+(?:that\s+)?"
            r"(?:they|he|she)\s+(?:has|have)\s+"
            r"(?:performed|provided|used|handled|completed)\b",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        if descriptive_tuple:
            entity_tables = named_tables(descriptive_tuple.group("entity"))
            if len(entity_tables) == 1:
                entity_table = entity_tables[0]
                left_bindings = exact_projection_bindings(
                    descriptive_tuple.group("left"), entity_table,
                )
                descriptor_tokens = self._normalized_language_tokens(
                    descriptive_tuple.group("right")
                )
                related_tokens = lexical_roots(
                    descriptive_tuple.group("related")
                )
                descriptor_candidates: List[tuple[str, str]] = []
                for table in self.schema.tables.values():
                    for column in table.columns:
                        column_tokens = self._normalized_language_tokens(column.name)
                        if not descriptor_tokens or not descriptor_tokens.issubset(
                            column_tokens
                        ):
                            continue
                        if not column_tokens & related_tokens:
                            continue
                        if unique_fk_path(entity_table, table.name) is not None:
                            descriptor_candidates.append((table.name, column.name))
                if len(left_bindings) == 1 and len(descriptor_candidates) == 1:
                    descriptor_table, descriptor_column = descriptor_candidates[0]
                    path = unique_fk_path(entity_table, descriptor_table)
                    if path:
                        bindings = [
                            left_bindings[0],
                            {"table": descriptor_table, "column": descriptor_column},
                        ]
                        if not contract.output_bindings:
                            contract.output_bindings.extend(bindings)
                            contract.output_columns.extend(
                                str(item["column"]) for item in bindings
                            )
                        distinct_requirement = {
                            "columns": [
                                f"{item['table']}.{item['column']}" for item in bindings
                            ],
                            "row_grain": "unique_output_tuple",
                        }
                        if distinct_requirement not in contract.distinct_row_requirements:
                            contract.distinct_row_requirements.append(
                                distinct_requirement
                            )
                        if path not in contract.relation_paths:
                            contract.relation_paths.append(path)
                        require(
                            "exact_output_projection",
                            "descriptive_related_tuple_projection",
                        )
                        require(
                            "distinct_output_tuple",
                            "each_entity_related_description_grain",
                        )

        # Definite singular arg-min/arg-max over one physical scalar becomes a
        # typed ordering node.  The plan is executable only when projection,
        # label filter, scalar and all FK joins are independently complete.
        scalar_superlative = re.search(
            r"\bthe\s+(?P<entity>[A-Za-z][\w-]*)\s+"
            r"(?:with|having)\s+(?:the\s+)?"
            r"(?P<direction>least|lowest|smallest|most|highest|largest)\s+"
            r"(?P<measure>[A-Za-z][\w-]*(?:\s+[A-Za-z][\w-]*){0,2}?)"
            r"(?=\s+\b(?:does|do|did|has|have|had|is|are|was|were)\b|[?.]|$)",
            text,
            re.IGNORECASE,
        )
        if scalar_superlative and contract.tie_policy == "single_row":
            measure_refs = phrase_column_refs(scalar_superlative.group("measure"))
            if len(measure_refs) == 1:
                table_name, column_name = measure_refs[0]
                column = next(
                    item for item in self.schema.tables[table_name].columns
                    if item.name == column_name
                )
                if re.search(
                    r"(?:INT|REAL|NUM|DEC|FLOAT|DOUBLE)",
                    column.type or "",
                    re.IGNORECASE,
                ):
                    direction_word = scalar_superlative.group("direction").casefold()
                    contract.ordering_requirements.append({
                        "column": f"{table_name}.{column_name}",
                        "direction": (
                            "ASC" if direction_word in {
                                "least", "lowest", "smallest",
                            } else "DESC"
                        ),
                        "limit": 1,
                        "tie_policy": "single_row",
                    })
                    require("scalar_order", "explicit_schema_scalar_superlative")

        # A trailing modifier after "either A or B" has two plausible Boolean
        # scopes in ordinary language: A OR (B AND C), or (A OR B) AND C.
        # When A/B are values of one physical column and C binds to another
        # scalar on that same table, stop for clarification instead of silently
        # choosing whichever precedence the generated SQL happened to use.
        boolean_scope = re.search(
            r"\beither\s+(?P<left>[A-Za-z][\w-]*)\s+or\s+"
            r"(?P<right>[A-Za-z][\w-]*)\s+with\s+"
            r"(?P<modifier>(?P<operator>more\s+than|less\s+than|at\s+least|"
            r"at\s+most|exactly)\s+(?P<value>\d+)\s+"
            r"(?P<subject>[A-Za-z][\w-]*))(?=[?.；;]|$)",
            text,
            re.IGNORECASE,
        )
        if boolean_scope:
            category_values = [
                self._normalized_language_tokens(boolean_scope.group(name))
                for name in ("left", "right")
            ]
            category_columns: List[tuple[str, str]] = []
            for table in self.schema.tables.values():
                for column in table.columns:
                    sample_tokens = [
                        self._normalized_language_tokens(str(value))
                        for value in column.sample_values if value is not None
                    ]
                    if all(any(value == sample for sample in sample_tokens)
                           for value in category_values):
                        category_columns.append((table.name, column.name))
            modifier_binding = scalar_threshold_binding(
                boolean_scope.group("subject")
            )
            nearest_category = nearest_column_binding(
                category_columns,
                [modifier_binding[0]] if modifier_binding is not None else [],
            )
            category_binding = nearest_category[0] if nearest_category else None
            if category_binding is not None and modifier_binding is not None \
                    and category_binding[0] == modifier_binding[0]:
                category_schema = self.schema.tables[category_binding[0]]
                category_column = next(
                    column for column in category_schema.columns
                    if column.name == category_binding[1]
                )
                canonical_values: List[str] = []
                for expected_tokens, raw_value in zip(
                    category_values,
                    (boolean_scope.group("left"), boolean_scope.group("right")),
                ):
                    grounded = next((
                        str(value) for value in category_column.sample_values
                        if value is not None
                        and self._normalized_language_tokens(str(value))
                        == expected_tokens
                    ), raw_value)
                    canonical_values.append(grounded)
                resolution = re.search(
                    r"布尔筛选作用域\s*[:：]\s*(?P<value>.+?)\s*$",
                    text,
                    re.IGNORECASE,
                )
                resolution_text = (
                    resolution.group("value").strip() if resolution else ""
                )
                resolved_scope = (
                    "both_categories" if re.search(
                        r"同时|两个|两类|both|all",
                        resolution_text,
                        re.IGNORECASE,
                    )
                    else "right_category_only" if re.search(
                        r"只|后一|后面|right|second",
                        resolution_text,
                        re.IGNORECASE,
                    )
                    else ""
                )
                if resolved_scope:
                    contract.boolean_filter_requirements.append({
                        "category_column": (
                            f"{category_binding[0]}.{category_binding[1]}"
                        ),
                        "category_values": canonical_values,
                        "modifier_column": (
                            f"{modifier_binding[0]}.{modifier_binding[1]}"
                        ),
                        "modifier_operator": operator_by_phrase[
                            re.sub(
                                r"\s+", " ",
                                boolean_scope.group("operator").casefold(),
                            )
                        ],
                        "modifier_value": int(boolean_scope.group("value")),
                        "scope": resolved_scope,
                    })
                    require(
                        "boolean_filter_scope",
                        "user_resolved_either_or_modifier",
                    )
                else:
                    contract.ambiguities.append({
                        "kind": "boolean_modifier_scope",
                        "category_column": (
                            f"{category_binding[0]}.{category_binding[1]}"
                        ),
                        "category_values": canonical_values,
                        "modifier_column": (
                            f"{modifier_binding[0]}.{modifier_binding[1]}"
                        ),
                        "modifier": boolean_scope.group("modifier"),
                        "choices": [
                            "修饰条件只作用于 or 后面的类别",
                            "修饰条件同时作用于两个类别",
                        ],
                    })
                    require(
                        "clarify_boolean_scope",
                        "ambiguous_either_or_modifier",
                    )

        # General "both A and B" requests require the two values at the output
        # entity's grain.  Bind only when the values are explicit, at least one
        # value is grounded in one unique sampled physical column, the output
        # grain is a declared key and the schema provides one relation path.
        # The accompanying literal policy prevents a candidate from adding an
        # unrelated restrictive value (for example a tournament round) that
        # was never stated by the user or compiled from schema semantics.
        explicit_both_values: List[str] = []
        quoted_both = re.search(
            r"\bboth\b.{0,80}?['\"](?P<left>[^'\"]{1,80})['\"]\s+"
            r"and\s+(?:the\s+[A-Za-z][\w-]*\s+)?"
            r"['\"](?P<right>[^'\"]{1,80})['\"]",
            text,
            re.IGNORECASE | re.DOTALL,
        )
        named_both = None if quoted_both else re.search(
            r"\bboth\s+(?:the\s+)?"
            r"(?P<left>[A-Z][A-Za-z0-9&.'-]*"
            r"(?:\s+[A-Z][A-Za-z0-9&.'-]*){0,5})\s+and\s+(?:the\s+)?"
            r"(?P<right>[A-Z][A-Za-z0-9&.'-]*"
            r"(?:\s+[A-Z][A-Za-z0-9&.'-]*){0,5})(?=[?.]|$)",
            text,
        )
        both_match = quoted_both or named_both
        if both_match:
            explicit_both_values = [
                re.sub(r"\s+", " ", both_match.group(name)).strip()
                for name in ("left", "right")
            ]
            grounded_columns: List[tuple[int, str, str]] = []
            for table in self.schema.tables.values():
                for column in table.columns:
                    samples = {
                        re.sub(r"\s+", " ", str(value)).strip().casefold()
                        for value in column.sample_values if value is not None
                    }
                    matches = sum(
                        value.casefold() in samples for value in explicit_both_values
                    )
                    if matches:
                        grounded_columns.append((matches, table.name, column.name))
            if grounded_columns:
                best_grounding = max(item[0] for item in grounded_columns)
                best_columns = [
                    item for item in grounded_columns if item[0] == best_grounding
                ]
            else:
                best_columns = []

            parent_tables = list(dict.fromkeys(
                str(item.get("table") or "") for item in contract.output_bindings
                if item.get("table")
            ))
            if not parent_tables:
                output_prefix = re.split(
                    r"\b(?:in\s+which|where|who|that)\b",
                    text[:both_match.start()],
                    maxsplit=1,
                    flags=re.IGNORECASE,
                )[0]
                output_refs = phrase_column_refs(output_prefix)
                if len(output_refs) == 1:
                    output_table, output_column = output_refs[0]
                    contract.output_bindings.append({
                        "table": output_table, "column": output_column,
                    })
                    contract.output_columns.append(output_column)
                    parent_tables = [output_table]
                    require(
                        "exact_output_projection",
                        "all_values_output_grain_projection",
                    )

            if len(best_columns) == 1 and len(parent_tables) == 1:
                _matched_count, value_table, value_column = best_columns[0]
                parent_table = parent_tables[0]
                path = unique_fk_path(parent_table, value_table)
                if path is None and parent_table != value_table:
                    # Role-named direct FKs (winner/loser, sender/receiver) can
                    # make a shortest path structurally ambiguous.  The verb
                    # immediately before "both" may disambiguate one physical
                    # edge without inventing a relationship.
                    relation_tokens = lexical_roots(text[:both_match.start()])
                    role_aliases = {
                        "won": "winner", "win": "winner",
                        "lost": "loser", "lose": "loser",
                        "sent": "sender", "received": "receiver",
                    }
                    relation_tokens.update(
                        role_aliases[token] for token in list(relation_tokens)
                        if token in role_aliases
                    )
                    role_edges = []
                    for column in self.schema.tables[value_table].columns:
                        if str(column.fk_table or "").casefold() \
                                != parent_table.casefold() or not column.fk_column:
                            continue
                        if self._normalized_language_tokens(column.name) & relation_tokens:
                            role_edges.append({
                                "from": f"{value_table}.{column.name}",
                                "to": f"{parent_table}.{column.fk_column}",
                                "source": "foreign_key",
                            })
                    if len(role_edges) == 1:
                        path = {
                            "tables": [parent_table, value_table],
                            "edges": role_edges,
                            "source": "role_bound_declared_fk_path",
                        }
                parent_keys = [
                    column for column in self.schema.tables[parent_table].columns
                    if column.pk
                ]
                if path and len(parent_keys) == 1:
                    parent_key = f"{parent_table}.{parent_keys[0].name}"
                    requirement = {
                        "operator": "ALL_VALUES",
                        "fact_table": value_table,
                        "value_column": f"{value_table}.{value_column}",
                        "values": explicit_both_values,
                        "parent_table": parent_table,
                        "parent_key": parent_key,
                        "relation_path": path,
                        "row_grain": parent_key,
                    }
                    if requirement not in contract.set_requirements:
                        contract.set_requirements.append(requirement)
                    if path not in contract.relation_paths:
                        contract.relation_paths.append(path)
                    literal_policy = {
                        "mode": "question_grounded_string_predicates",
                        "allowed_values": explicit_both_values,
                        "value_column": f"{value_table}.{value_column}",
                    }
                    if literal_policy not in contract.predicate_literal_policies:
                        contract.predicate_literal_policies.append(literal_policy)
                    require("all_values", "explicit_same_output_grain_all_values")
                    require(
                        "predicate_literal_provenance",
                        "explicit_values_close_candidate_predicates",
                    )

        # "directed by A and B" describes two fact values that the same parent
        # entity must possess; a single IN predicate means A OR B and is not
        # equivalent.  Bind proper-name values only when the relation phrase
        # resolves to one physical fact column and one declared FK connects that
        # fact table to the already bound output entity.
        all_values_match = re.search(
            r"\b(?P<relation>[A-Za-z][\w-]*\s+by)\s+"
            r"(?P<left>[A-Z][\w'-]+(?:\s+[A-Z][\w'-]+){1,3})\s+and\s+"
            r"(?P<right>[A-Z][\w'-]+(?:\s+[A-Z][\w'-]+){1,3})"
            r"(?=[?.]|$)",
            text,
        )
        if all_values_match is None:
            all_values_match = re.search(
                r"\b(?P<relation>[A-Za-z][\w-]*\s+by)\s+"
                r"(?P<left>[A-Z][\w'-]+(?:\s+[A-Z][\w'-]+){1,3})\s+and\s+"
                r"(?:[A-Za-z][\w-]*\s+){0,2}(?P=relation)\s+"
                r"(?P<right>[A-Z][\w'-]+(?:\s+[A-Z][\w'-]+){1,3})"
                r"(?=[?.]|$)",
                text,
                re.IGNORECASE,
            )
        if all_values_match and not contract.set_requirements:
            value_refs = phrase_column_refs(all_values_match.group("relation"))
            parent_tables = list(dict.fromkeys(
                str(item.get("table") or "") for item in contract.output_bindings
                if item.get("table")
            ))
            if len(value_refs) == 1 and len(parent_tables) == 1:
                fact_table, value_column = value_refs[0]
                parent_table = parent_tables[0]
                parent_keys = [
                    column for column in self.schema.tables[parent_table].columns
                    if column.pk
                ]
                fact_links = [
                    column for column in self.schema.tables[fact_table].columns
                    if column.fk_table == parent_table and column.fk_column
                    and any(
                        key.name.casefold() == column.fk_column.casefold()
                        for key in parent_keys
                    )
                ]
                if fact_table != parent_table and len(parent_keys) == 1 \
                        and len(fact_links) == 1:
                    contract.set_requirements.append({
                        "operator": "ALL_VALUES",
                        "fact_table": fact_table,
                        "value_column": f"{fact_table}.{value_column}",
                        "values": [
                            all_values_match.group("left"),
                            all_values_match.group("right"),
                        ],
                        "parent_table": parent_table,
                        "parent_key": f"{parent_table}.{parent_keys[0].name}",
                        "fact_parent_key": f"{fact_table}.{fact_links[0].name}",
                        "proof_edge": {
                            "from": f"{fact_table}.{fact_links[0].name}",
                            "to": f"{parent_table}.{parent_keys[0].name}",
                            "source": "foreign_key",
                        },
                    })
                    require("all_values", "explicit_same_entity_all_values")

        if contract.output_bindings and not contract.output_layout \
                and "explicit_dictionary_tuple" not in contract.evidence:
            ordered_bindings = self._order_output_bindings_by_question(
                output_phrase, contract.output_bindings,
            )
            if ordered_bindings != contract.output_bindings:
                contract.output_bindings = ordered_bindings
                if len(contract.output_columns) == len(ordered_bindings) \
                        and {
                            str(item.get("column") or "").casefold()
                            for item in ordered_bindings
                        } == {item.casefold() for item in contract.output_columns}:
                    contract.output_columns = [
                        str(item["column"]) for item in ordered_bindings
                    ]
                require(
                    "exact_output_projection",
                    "question_ordered_output_projection",
                )

        # Reconcile display ownership with the independently compiled result
        # identity.  This catches the dangerous case where generic ``id`` and
        # ``name`` tokens bind to one table while aggregation is performed for
        # another entity.  The compiler never guesses which side should win.
        if contract.result_grain and contract.output_bindings:
            owner_table = str(contract.result_grain.get("owner_table") or "")
            visible_owners = list(dict.fromkeys(
                str(item.get("table") or "")
                for item in contract.output_bindings if item.get("table")
            ))
            if owner_table and visible_owners != [owner_table]:
                ambiguity = {
                    "kind": "result_grain_output_owner_conflict",
                    "owner_table": owner_table,
                    "visible_tables": visible_owners,
                }
                if ambiguity not in contract.ambiguities:
                    contract.ambiguities.append(ambiguity)
            else:
                contract.result_grain["visible_columns"] = [
                    f"{item['table']}.{item['column']}"
                    for item in contract.output_bindings
                    if item.get("table") and item.get("column")
                ]

        # ALL_VALUES is a set-valued entity predicate. If the final projection
        # intentionally drops that entity's key, multiple qualifying entities
        # can collapse to the same visible tuple. Preserve set semantics by
        # requiring DISTINCT (or an equivalent set/group operation) over the
        # complete visible tuple rather than leaking duplicate fact/entity rows.
        for requirement in contract.set_requirements:
            if requirement.get("operator") != "ALL_VALUES":
                continue
            parent_table = str(requirement.get("parent_table") or "")
            parent_key = str(requirement.get("parent_key") or "").casefold()
            visible_bindings = [
                item for item in contract.output_bindings
                if str(item.get("table") or "") == parent_table
                and item.get("column")
            ]
            visible_columns = [
                f"{item['table']}.{item['column']}" for item in visible_bindings
            ]
            if parent_table and parent_key:
                declare_result_grain(
                    kind="entity",
                    owner_table=parent_table,
                    identity_columns=[str(requirement.get("parent_key") or "")],
                    visible_bindings=visible_bindings,
                    cardinality="set",
                    multiplicity=(
                        "unique_visible_tuple"
                        if parent_key not in {
                            value.casefold() for value in visible_columns
                        }
                        else "one_row_per_entity"
                    ),
                    source="all_values_entity_identity",
                )
            if visible_columns and len(visible_bindings) == len(contract.output_bindings) \
                    and parent_key not in {
                        value.casefold() for value in visible_columns
                    }:
                distinct_requirement = {
                    "columns": visible_columns,
                    "row_grain": "unique_output_tuple",
                    "source_grain": str(requirement.get("row_grain") or parent_key),
                }
                if distinct_requirement not in contract.distinct_row_requirements:
                    contract.distinct_row_requirements.append(distinct_requirement)
                require(
                    "distinct_output_tuple",
                    "all_values_projection_preserves_set_semantics",
                )

        # A requested single winner must be reproducible when the primary
        # measure ties. Prefer the already proven entity grouping key; for a
        # scalar ranking, use the ordered measure's owning-table primary key.
        # If neither is uniquely provable, retain the older fail-closed scope
        # and do not invent a tie breaker.
        if contract.tie_policy == "single_row":
            tie_breakers = list(dict.fromkeys(contract.grouping_keys))
            if not tie_breakers and contract.ordering_requirements:
                ordering_tables = list(dict.fromkeys(
                    str(item.get("column") or "").split(".", 1)[0]
                    for item in contract.ordering_requirements
                    if "." in str(item.get("column") or "")
                ))
                if len(ordering_tables) == 1 \
                        and ordering_tables[0] in self.schema.tables:
                    tie_breakers = [
                        f"{ordering_tables[0]}.{column.name}"
                        for column in self.schema.tables[ordering_tables[0]].columns
                        if column.pk
                    ]
            if not tie_breakers:
                output_tables = list(dict.fromkeys(
                    str(item.get("table") or "")
                    for item in contract.output_bindings if item.get("table")
                ))
                if len(output_tables) == 1 and output_tables[0] in self.schema.tables:
                    tie_breakers = [
                        f"{output_tables[0]}.{column.name}"
                        for column in self.schema.tables[output_tables[0]].columns
                        if column.pk
                    ]
            if tie_breakers:
                contract.tie_breaker_columns.extend(tie_breakers)
                require(
                    "deterministic_tie_breaker",
                    "single_row_superlative_reproducibility",
                )

        required_tables: List[str] = []

        def add_required_table(name: str) -> None:
            if name in self.schema.tables and name not in required_tables:
                required_tables.append(name)

        for binding in contract.output_bindings:
            add_required_table(str(binding.get("table") or ""))
        for qualified in contract.grouping_keys:
            add_required_table(qualified.split(".", 1)[0])
        for requirement in contract.aggregate_requirements:
            add_required_table(str(requirement.get("column") or "").split(".", 1)[0])
        for requirement in contract.filter_requirements:
            add_required_table(str(requirement.get("column") or "").split(".", 1)[0])
        for requirement in contract.boolean_filter_requirements:
            add_required_table(
                str(requirement.get("category_column") or "").split(".", 1)[0]
            )
            add_required_table(
                str(requirement.get("modifier_column") or "").split(".", 1)[0]
            )
        for requirement in contract.ordering_requirements:
            add_required_table(str(requirement.get("column") or "").split(".", 1)[0])
        for requirement in contract.ratio_requirements:
            for table_name in requirement.get("population_tables") or []:
                add_required_table(str(table_name))

        if len(required_tables) >= 2:
            anchor = required_tables[0]
            for target in required_tables[1:]:
                path = unique_fk_path(anchor, target)
                if path and path not in contract.relation_paths:
                    contract.relation_paths.append(path)
            if contract.relation_paths:
                require("relation_path", "unique_declared_fk_path")

        # Exact table mentions are useful even when the projection is a
        # descriptive column, but a mention alone does not prove that every
        # table belongs in one joined branch (set operations are a counterexample).
        # Record the unique physical path as a conditional contract: validate an
        # edge whenever both of its endpoint tables occur in the SQL, without
        # forcing an otherwise unnecessary bridge/table into the query.
        exact_tables = explicit_named_tables(text)
        if 2 <= len(exact_tables) <= 4:
            anchor = exact_tables[0]
            for target in exact_tables[1:]:
                path = unique_fk_path(anchor, target)
                correlation_edges = [
                    edge
                    for item in contract.correlation_requirements
                    for edge in (item.get("path") or {}).get("edges") or []
                ]
                set_proof_edges = [
                    proof
                    for requirement in contract.set_requirements
                    for proof in [requirement.get("proof_edge") or {}]
                    if proof.get("from") and proof.get("to")
                ]
                if not path or any(
                    existing.get("edges") == path.get("edges")
                    for existing in [
                        *contract.relation_paths,
                        *[
                            item.get("path") or {}
                            for item in contract.correlation_requirements
                        ],
                    ]
                ) or any(
                    edge in correlation_edges for edge in path.get("edges") or []
                ) or any(
                    edge in set_proof_edges for edge in path.get("edges") or []
                ):
                    continue
                conditional_path = dict(path)
                conditional_path["source"] = (
                    "question_named_unique_shortest_declared_fk_path"
                )
                conditional_path["enforcement"] = "when_both_tables_referenced"
                contract.relation_paths.append(conditional_path)
            if contract.relation_paths:
                require("relation_path", "question_named_schema_path")
        return contract

    @staticmethod
    def _plan_column_ref(value: Any) -> Optional[RelationalColumnRef]:
        raw = str(value or "")
        if raw.count(".") != 1:
            return None
        table_name, column_name = raw.split(".", 1)
        if not table_name or not column_name:
            return None
        return RelationalColumnRef(table=table_name, column=column_name)

    def _is_closed_counted_relationship_question(
        self,
        question: str,
        plan_refs: List[RelationalColumnRef],
        sources: List[str],
        filters: Optional[List[RelationalFilterPredicate]] = None,
        semantic_evidence: Optional[List[str]] = None,
    ) -> bool:
        """Reject local compilation when any question token is unrepresented.

        The counted/category plan represents only bounded typed filters.  This
        closed vocabulary check prevents a syntactically complete COUNT/rank
        plan from silently dropping any remaining year, threshold, location or
        status condition.
        """
        text, separator, evidence = str(question or "").partition(
            "Relevant business evidence supplied by the user:"
        )
        if separator and evidence.strip():
            return False
        relationship_match = self._COUNTED_RELATIONSHIP_SUPERLATIVE_RE.search(text)
        is_category_usage = bool(
            semantic_evidence
            and "category_usage_physical_projection" in semantic_evidence
        )
        if relationship_match is None and not is_category_usage:
            return False
        allowed = self._normalized_language_tokens(
            "a all an and are by conduct conducted each find for get give had has have "
            "including in is least list made make most number of one produce produced "
            "receive received return show single the that tie tied use used using "
             "visit visited was were what which who win with won fewest count me "
             "code codes name names id ids identifier identifiers type commonly "
             "frequently often selected chosen found register registered enroll enrolled"
             " time times"
         )
        for table_name in sources:
            allowed.update(self._normalized_language_tokens(table_name))
        for ref in plan_refs:
            allowed.update(self._normalized_language_tokens(ref.column))
        if filters:
            allowed.update(self._normalized_language_tokens(
                "after before earlier in later or year",
            ))
            for item in filters:
                allowed.update(self._normalized_language_tokens(item.column.column))
                allowed.update(self._normalized_language_tokens(str(item.value)))
        # A user may name a business event and then explicitly name the fact
        # table that records it, e.g. "most awards in evaluations".  The event
        # noun is safe closed-world evidence only when the post-superlative
        # phrase also contains one of the plan's physical sources; otherwise
        # the native plan still fails closed rather than silently dropping it.
        relation_tail = text[relationship_match.end():] if relationship_match else ""
        if relationship_match and any(
            self._normalized_language_tokens(source)
            and self._normalized_language_tokens(source).issubset(
                self._normalized_language_tokens(relation_tail)
            )
            for source in sources[1:]
        ):
            allowed.update(self._normalized_language_tokens(
                relationship_match.group("relation")
            ))
            allowed.add("in")
        return self._normalized_language_tokens(text).issubset(allowed)

    def _is_closed_scalar_ranking_question(
        self,
        question: str,
        refs: List[RelationalColumnRef],
        sources: List[str],
        filters: List[RelationalFilterPredicate],
    ) -> bool:
        """Require every token in a native arg-min/arg-max request to be represented."""
        text, separator, evidence = str(question or "").partition(
            "Relevant business evidence supplied by the user:"
        )
        if separator and evidence.strip():
            return False
        allowed = self._normalized_language_tokens(
            "a all an and are does do did for has have had how in is many most "
            "least lowest highest smallest largest of one return show the version "
            "what which with was were"
        )
        for table_name in sources:
            allowed.update(self._normalized_language_tokens(table_name))
        for ref in refs:
            allowed.update(self._normalized_language_tokens(ref.column))
        for item in filters:
            allowed.update(self._normalized_language_tokens(item.column.column))
            allowed.update(self._normalized_language_tokens(str(item.value)))
        return self._normalized_language_tokens(text).issubset(allowed)

    def _is_closed_grouped_relationship_count_question(self, question: str) -> bool:
        """Accept only the complete bounded entity-label plus count grammar."""
        text, separator, evidence = str(question or "").partition(
            "Relevant business evidence supplied by the user:"
        )
        if separator and evidence.strip():
            return False
        return self._GROUPED_RELATIONSHIP_COUNT_RE.fullmatch(text.strip()) is not None

    @staticmethod
    def _grouped_metrics_numeric_column(column: DBColumn) -> bool:
        return bool(re.search(
            r"(?:INT|REAL|FLOA|DOUB|DEC|NUM|MONEY|SERIAL)",
            str(column.type or ""),
            re.IGNORECASE,
        ))

    def _compile_grouped_metrics_plan(
        self,
        question: str,
        contract: RelationalAlgebraContract,
        allowed_tables: Optional[List[str]] = None,
    ) -> Optional[RelationalGroupedMetricsPlan]:
        """Compile a bounded physical GROUP BY request without model-owned SQL.

        The compiler deliberately accepts only one exact physical dimension,
        two to six aggregates over one fact table, typed equality/range filters
        and either a unique declared FK or an exact user-supplied equality.
        Those restrictions make projection, row grain and join cardinality part
        of a validated plan instead of prompt conventions.
        """
        connector = getattr(self.security, "connector", None)
        dialect = str(getattr(connector, "dialect", "sqlite") or "sqlite").lower()
        if dialect not in {"sqlite", "mysql", "postgresql"}:
            return None
        text = str(question or "")
        allowed = (
            {str(name).casefold() for name in allowed_tables}
            if allowed_tables is not None else None
        )

        columns_by_name: Dict[str, List[RelationalColumnRef]] = {}
        column_objects: Dict[tuple[str, str], DBColumn] = {}
        for table_name, table in self.schema.tables.items():
            if allowed is not None and table_name.casefold() not in allowed:
                continue
            for column in table.columns:
                ref = RelationalColumnRef(table_name, column.name)
                columns_by_name.setdefault(column.name.casefold(), []).append(ref)
                column_objects[(table_name, column.name)] = column

        def exact_refs(fragment: str) -> List[RelationalColumnRef]:
            found: List[RelationalColumnRef] = []
            for token in re.findall(r"[A-Za-z_][\w$]*", fragment or ""):
                matches = columns_by_name.get(token.casefold(), [])
                if len(matches) == 1 and matches[0] not in found:
                    found.append(matches[0])
            return found

        grouping_match = re.search(
            r"(?:按|根据)\s*(?P<dimension>[^\uff0c,\u3002.;]{1,48}?)\s*"
            r"(?:统计|分组|汇总)",
            text,
            re.IGNORECASE,
        )
        if grouping_match is None:
            grouping_match = re.search(
                r"(?:group(?:ed)?|breakdown)\s+by\s+"
                r"(?P<dimension>[A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)?)",
                text,
                re.IGNORECASE,
            )
        if grouping_match is None:
            return None
        dimension_refs = exact_refs(grouping_match.group("dimension"))
        if len(dimension_refs) != 1:
            return None
        dimension = dimension_refs[0]

        explicit_edges: List[tuple[RelationalJoinEdge, tuple[int, int]]] = []
        explicit_spans: List[tuple[int, int]] = []
        for match in SchemaRelationAnalyzer._EXPLICIT_RELATION_RE.finditer(text):
            left_table, left_column, right_table, right_column = match.groups()
            if left_table == right_table:
                continue
            left = RelationalColumnRef(left_table, left_column)
            right = RelationalColumnRef(right_table, right_column)
            if (left.table, left.column) not in column_objects \
                    or (right.table, right.column) not in column_objects:
                continue
            explicit_edges.append((RelationalJoinEdge(
                left=left, right=right, source="explicit",
            ), match.span()))
            explicit_spans.append(match.span())

        filters: List[RelationalFilterPredicate] = []
        filter_refs: List[RelationalColumnRef] = []
        filter_re = re.compile(
            r"(?:(?P<table>[A-Za-z_][\w$]*)\s*\.\s*)?"
            r"(?P<column>[A-Za-z_][\w$]*)\s*"
            r"(?P<operator>!=|<>|>=|<=|=|>|<)\s*"
            r"(?P<value>'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|-?\d+(?:\.\d+)?)",
            re.IGNORECASE,
        )
        for match in filter_re.finditer(text):
            if any(start <= match.start() and match.end() <= end for start, end in explicit_spans):
                continue
            table_hint = str(match.group("table") or "")
            column_name = match.group("column")
            candidates = columns_by_name.get(column_name.casefold(), [])
            if table_hint:
                candidates = [
                    ref for ref in candidates
                    if ref.table.casefold() == table_hint.casefold()
                ]
            if len(candidates) != 1:
                return None
            raw_value = match.group("value")
            if raw_value.startswith(("'", '"')):
                value = raw_value[1:-1].replace("''", "'").replace('""', '"')
                value_type = "text"
            else:
                value = float(raw_value) if "." in raw_value else int(raw_value)
                value_type = "number"
            predicate = RelationalFilterPredicate(
                column=candidates[0],
                operator=match.group("operator"),
                value=value,
                value_type=value_type,
            )
            if predicate not in filters:
                filters.append(predicate)
                filter_refs.append(candidates[0])

        numeric_aggregates: List[RelationalAggregate] = []
        aggregate_refs: List[RelationalColumnRef] = []
        occupied_refs = {dimension, *filter_refs}
        amount_name_re = re.compile(
            r"(?:amount|total|revenue|sales|price|cost|fee|balance)",
            re.IGNORECASE,
        )
        cue_map = (
            ("SUM", re.compile(r"(?:合计|总和|求和|总额|\bsum\b)", re.IGNORECASE)),
            ("AVG", re.compile(r"(?:平均|\bavg\b|\baverage\b)", re.IGNORECASE)),
            ("MAX", re.compile(r"(?:最大|最高|\bmax\b|\bmaximum\b)", re.IGNORECASE)),
            ("MIN", re.compile(r"(?:最小|最低|\bmin\b|\bminimum\b)", re.IGNORECASE)),
        )
        for folded_name, candidates in columns_by_name.items():
            if len(candidates) != 1:
                continue
            ref = candidates[0]
            if ref in occupied_refs:
                continue
            column = column_objects[(ref.table, ref.column)]
            if not self._grouped_metrics_numeric_column(column):
                continue
            mention = re.search(
                rf"(?<![A-Za-z0-9_$]){re.escape(ref.column)}(?![A-Za-z0-9_$])",
                text,
                re.IGNORECASE,
            )
            if mention is None or any(
                start <= mention.start() and mention.end() <= end
                for start, end in explicit_spans
            ):
                continue
            window = text[max(0, mention.start() - 16):min(len(text), mention.end() + 16)]
            function = next((name for name, cue in cue_map if cue.search(window)), "")
            if not function and amount_name_re.search(ref.column) \
                    and re.search(r"(?:统计|汇总|aggregate|metric)", text, re.IGNORECASE):
                function = "SUM"
            if not function:
                continue
            alias = re.sub(r"[^A-Za-z0-9_$]+", "_", ref.column).strip("_")
            aggregate = RelationalAggregate(
                function=function,
                source_table=ref.table,
                column=ref.column,
                alias=f"{alias}_{function.lower()}",
            )
            numeric_aggregates.append(aggregate)
            aggregate_refs.append(ref)

        count_requested = bool(re.search(
            r"(?:订单数|记录数|行数|数据量|数量|\bcount(?:\s+of)?\b|"
            r"\bnumber\s+of\b)",
            text,
            re.IGNORECASE,
        ))
        fact_candidates = list(dict.fromkeys(
            [ref.table for ref in aggregate_refs]
            + [ref.table for ref in filter_refs if ref.table != dimension.table]
        ))
        named_tables = [
            table_name for table_name in self.schema.tables
            if re.search(
                rf"(?<![A-Za-z0-9_$]){re.escape(table_name)}(?![A-Za-z0-9_$])",
                text,
                re.IGNORECASE,
            )
        ]
        if not fact_candidates:
            fact_candidates = [name for name in named_tables if name != dimension.table]
        if not fact_candidates:
            fact_candidates = [dimension.table]
        if len(set(fact_candidates)) != 1:
            return None
        fact_table = fact_candidates[0]
        if any(item.source_table != fact_table for item in numeric_aggregates):
            return None

        aggregates = list(numeric_aggregates)
        if count_requested:
            aggregates.insert(0, RelationalAggregate(
                function="COUNT",
                source_table=fact_table,
                column="*",
                alias=f"{fact_table}_count",
            ))
        if not 2 <= len(aggregates) <= 6:
            return None

        referenced_tables = list(dict.fromkeys(
            [dimension.table, fact_table]
            + [item.column.table for item in filters]
        ))
        if len(referenced_tables) > 2 or any(
            table_name not in {dimension.table, fact_table}
            for table_name in referenced_tables
        ):
            return None

        joins: List[RelationalJoinEdge] = []
        sources = [dimension.table]
        if fact_table != dimension.table:
            matching_explicit = [
                edge for edge, _span in explicit_edges
                if {edge.left.table, edge.right.table} == {dimension.table, fact_table}
            ]
            declared: List[RelationalJoinEdge] = []
            for table in self.schema.tables.values():
                for column in table.columns:
                    if not column.fk_table or not column.fk_column:
                        continue
                    if {table.name, column.fk_table} != {dimension.table, fact_table}:
                        continue
                    declared.append(RelationalJoinEdge(
                        left=RelationalColumnRef(table.name, column.name),
                        right=RelationalColumnRef(column.fk_table, column.fk_column),
                        source="foreign_key",
                    ))
            if matching_explicit:
                if len(matching_explicit) != 1:
                    return None
                selected = matching_explicit[0]
                if any(
                    {selected.left, selected.right} == {edge.left, edge.right}
                    for edge in declared
                ):
                    selected = next(
                        edge for edge in declared
                        if {selected.left, selected.right} == {edge.left, edge.right}
                    )
            elif len(declared) == 1:
                selected = declared[0]
            else:
                return None
            joins = [selected]
            sources.append(fact_table)

        direction = "ASC"
        order_target = ""
        order_match = re.search(
            r"(?:按|by)\s*(?P<target>[^\uff0c,\u3002.;]{1,32}?)\s*"
            r"(?P<direction>降序|升序|排序|desc(?:ending)?|asc(?:ending)?)",
            text,
            re.IGNORECASE,
        )
        if order_match:
            order_target = order_match.group("target").strip()
            raw_direction = order_match.group("direction").casefold()
            direction = "DESC" if raw_direction.startswith("desc") or raw_direction == "降序" else "ASC"
        order_by: List[RelationalOrderTerm] = []
        if order_target:
            target_refs = exact_refs(order_target)
            if target_refs == [dimension]:
                order_by.append(RelationalOrderTerm(direction=direction, column=dimension))
            else:
                selected_aggregate: Optional[RelationalAggregate] = None
                for aggregate in aggregates:
                    if aggregate.column != "*" and re.search(
                        rf"(?<![A-Za-z0-9_$]){re.escape(aggregate.column)}(?![A-Za-z0-9_$])",
                        order_target,
                        re.IGNORECASE,
                    ):
                        selected_aggregate = aggregate
                        break
                if selected_aggregate is None and re.search(
                    r"(?:金额|总额|amount|total|sum)", order_target, re.IGNORECASE,
                ):
                    sums = [item for item in aggregates if item.function == "SUM"]
                    selected_aggregate = sums[0] if len(sums) == 1 else None
                if selected_aggregate is None and re.search(
                    r"(?:数量|订单数|count|number)", order_target, re.IGNORECASE,
                ):
                    counts = [item for item in aggregates if item.function == "COUNT"]
                    selected_aggregate = counts[0] if len(counts) == 1 else None
                if selected_aggregate is None:
                    return None
                order_by.append(RelationalOrderTerm(
                    direction=direction,
                    aggregate_alias=selected_aggregate.alias,
                ))
        if not order_by:
            order_by = [RelationalOrderTerm(direction="ASC", column=dimension)]

        evidence = [
            "exact_physical_group_dimension",
            "bounded_typed_multi_aggregate",
        ]
        if filters:
            evidence.append("typed_literal_filters")
        if joins:
            evidence.append(
                "declared_fk_relation" if joins[0].source == "foreign_key"
                else "user_explicit_equality_relation"
            )
        return RelationalGroupedMetricsPlan(
            sources=sources,
            joins=joins,
            dimensions=[dimension],
            group_keys=[dimension],
            aggregates=aggregates,
            filters=filters,
            order_by=order_by,
            contract_version=contract.version,
            evidence=evidence,
            dialect=dialect,
        )

    def _compile_native_relational_plan(
        self,
        question: str,
        contract: RelationalAlgebraContract,
        allowed_tables: Optional[List[str]] = None,
    ) -> Optional[Any]:
        """Compile one proven relational-IR slice to a typed executable plan.

        Coverage is intentionally bounded: validated set/difference, distinct
        entity count, scalar arg-extremum, and counted/category relationship
        ranking slices.  Every slice requires exact physical projections,
        declared FK evidence and complete cardinality/filter semantics.  Any
        incomplete or extra semantic node returns ``None`` and leaves the
        existing model-plus-gates path unchanged.
        """
        connector = getattr(self.security, "connector", None)
        if not isinstance(connector, DBConnector):
            return None
        allowed = (
            {str(name).casefold() for name in allowed_tables}
            if allowed_tables is not None else None
        )

        # Result identity, visible ownership and aggregate subjects are one
        # typed plan boundary in 1.9.  Refuse local execution if older helper
        # paths produced individually plausible but mutually inconsistent
        # fragments; the model path may still run through the same semantic
        # validator, but the deterministic compiler cannot own a partial plan.
        if contract.result_grain:
            result_owner = str(contract.result_grain.get("owner_table") or "")
            identity_columns = [
                str(item) for item in (
                    contract.result_grain.get("identity_columns") or []
                )
            ]
            visible_owners = {
                str(item.get("table") or "")
                for item in contract.output_bindings if item.get("table")
            }
            if result_owner and visible_owners and visible_owners != {result_owner}:
                return None
            group_stage = next((
                stage for stage in contract.aggregation_stages
                if stage.get("kind") == "group_aggregate"
            ), None)
            if group_stage is not None and identity_columns \
                    and [str(item).casefold() for item in group_stage.get("group_keys") or []] \
                    != [item.casefold() for item in identity_columns]:
                return None
        for subject in contract.aggregate_subjects:
            matching_stage = any(
                str(aggregate.get("function") or "").upper()
                == str(subject.get("function") or "").upper()
                and str(aggregate.get("source_table") or "")
                == str(subject.get("source_table") or "")
                and str(aggregate.get("column") or "")
                == str(subject.get("column") or "")
                for stage in contract.aggregation_stages
                if stage.get("kind") == "group_aggregate"
                for aggregate in stage.get("aggregates") or []
            )
            if contract.aggregation_stages and not matching_stage:
                return None

        if len(contract.set_requirements) == 1 \
                and not contract.ratio_requirements \
                and not contract.correlation_requirements \
                and not contract.relationship_thresholds \
                and not contract.aggregation_stages:
            requirement = contract.set_requirements[0]
            branches: List[RelationalSetBranch] = []
            for raw in requirement.get("branches") or []:
                table_name = str(raw.get("table") or "")
                column_name = str(raw.get("column") or "")
                if not table_name or not column_name \
                        or table_name not in self.schema.tables \
                        or (allowed is not None and table_name.casefold() not in allowed):
                    return None
                branches.append(RelationalSetBranch(
                    source=table_name,
                    projection=RelationalColumnRef(table_name, column_name),
                ))
            operator = str(requirement.get("operator") or "").upper()
            if len(branches) >= 2 and operator in {"INTERSECT", "EXCEPT"}:
                proof_edges: List[RelationalJoinEdge] = []
                if operator == "EXCEPT":
                    proof = requirement.get("proof_edge") or {}
                    left = self._plan_column_ref(proof.get("from"))
                    right = self._plan_column_ref(proof.get("to"))
                    if left is None or right is None:
                        return None
                    proof_edges.append(RelationalJoinEdge(left=left, right=right))
                return RelationalSetQueryPlan(
                    operator=operator,
                    branches=branches,
                    output_name=str(requirement.get("output_name") or branches[0].projection.column),
                    contract_version=contract.version,
                    proof_edges=proof_edges,
                    evidence=[
                        "explicit_independent_set_semantics",
                        "schema_bound_branch_projections",
                        *(["declared_fk_anti_set"] if operator == "EXCEPT" else []),
                    ],
                )

        if len(contract.distinct_count_requirements) == 1 \
                and not contract.set_requirements \
                and not contract.ratio_requirements \
                and not contract.correlation_requirements \
                and not contract.relationship_thresholds \
                and not contract.aggregation_stages:
            requirement = contract.distinct_count_requirements[0]
            source = str(requirement.get("source_table") or "")
            column = self._plan_column_ref(requirement.get("column"))
            proof = requirement.get("proof_edge") or {}
            left = self._plan_column_ref(proof.get("from"))
            right = self._plan_column_ref(proof.get("to"))
            if source and column and left and right and column.table == source \
                    and source in self.schema.tables \
                    and (allowed is None or source.casefold() in allowed):
                return RelationalScalarAggregatePlan(
                    source=source,
                    aggregate=RelationalAggregate(
                        function="COUNT",
                        source_table=source,
                        column=column.column,
                        distinct=True,
                    ),
                    output_name="distinct_count",
                    proof_edges=[RelationalJoinEdge(left=left, right=right)],
                    contract_version=contract.version,
                    evidence=[
                        "explicit_distinct_entity_count",
                        "declared_fk_entity_key",
                    ],
                )

        if "grouped_relationship_count_physical_output" in contract.evidence \
                and len(contract.output_layout) == 2 \
                and len(contract.aggregation_stages) == 1 \
                and not contract.set_requirements \
                and not contract.distinct_count_requirements \
                and not contract.ratio_requirements \
                and not contract.correlation_requirements \
                and not contract.relationship_thresholds \
                and not contract.aggregate_requirements \
                and not contract.filter_requirements \
                and not contract.modifier_filters \
                and not contract.ordering_requirements \
                and not contract.comparison_quantifier \
                and not contract.comparison_direction:
            stage = contract.aggregation_stages[0]
            aggregates = stage.get("aggregates") or []
            paths = [
                path for path in contract.relation_paths
                if path.get("source") == "unique_shortest_declared_fk_path"
            ]
            if stage.get("kind") != "group_aggregate" \
                    or stage.get("stage") != 1 \
                    or stage.get("include_zero") is not True \
                    or len(aggregates) != 1 or len(paths) != 1:
                return None
            aggregate_spec = aggregates[0]
            if str(aggregate_spec.get("function") or "").upper() != "COUNT" \
                    or aggregate_spec.get("column") != "*" \
                    or aggregate_spec.get("output") is not True:
                return None
            path = paths[0]
            sources = [str(item) for item in path.get("tables") or []]
            fact_table = str(aggregate_spec.get("source_table") or "")
            raw_edges = list(path.get("edges") or [])
            if len(sources) < 2 or fact_table != sources[-1] \
                    or len(raw_edges) != len(sources) - 1 \
                    or any(name not in self.schema.tables for name in sources) \
                    or (allowed is not None and any(
                        name.casefold() not in allowed for name in sources
                    )):
                return None
            projections = [
                RelationalColumnRef(
                    table=str(binding.get("table") or ""),
                    column=str(binding.get("column") or ""),
                )
                for binding in contract.output_bindings
                if isinstance(binding, dict)
            ]
            group_keys = [
                self._plan_column_ref(item)
                for item in stage.get("group_keys") or []
            ]
            if not projections or any(ref is None for ref in group_keys):
                return None
            typed_group_keys = [ref for ref in group_keys if ref is not None]
            column_layout, aggregate_layout = contract.output_layout
            if column_layout.get("kind") != "column" \
                    or aggregate_layout.get("kind") != "aggregate" \
                    or str(aggregate_layout.get("function") or "").upper() != "COUNT" \
                    or aggregate_layout.get("source_table") != fact_table \
                    or [ref.column for ref in projections] != contract.output_columns \
                    or any(ref.table != sources[0] for ref in projections) \
                    or any(ref.table != sources[0] for ref in typed_group_keys) \
                    or [f"{ref.table}.{ref.column}" for ref in typed_group_keys] \
                    != list(contract.grouping_keys):
                return None
            joins: List[RelationalJoinEdge] = []
            for raw_edge in raw_edges:
                left = self._plan_column_ref(raw_edge.get("from"))
                right = self._plan_column_ref(raw_edge.get("to"))
                if left is None or right is None \
                        or raw_edge.get("source") != "foreign_key":
                    return None
                joins.append(RelationalJoinEdge(
                    left=left, right=right, join_type="LEFT",
                ))
            fact_keys = [
                column for column in self.schema.tables[fact_table].columns
                if column.pk
            ]
            if len(fact_keys) == 1:
                count_column = fact_keys[0].name
            else:
                fact_endpoints = [
                    ref for ref in (joins[-1].left, joins[-1].right)
                    if ref.table == fact_table
                ]
                if len(fact_endpoints) != 1:
                    return None
                count_column = fact_endpoints[0].column
            if not self._is_closed_grouped_relationship_count_question(question):
                return None
            return RelationalGroupedAggregatePlan(
                sources=sources,
                joins=joins,
                projections=projections,
                group_keys=typed_group_keys,
                aggregate=RelationalAggregate(
                    function="COUNT",
                    source_table=fact_table,
                    column=count_column,
                    alias=str(aggregate_layout.get("alias") or "related_count"),
                ),
                contract_version=contract.version,
                include_zero=True,
                evidence=[
                    "closed_question_shape",
                    "exact_physical_projection",
                    "unique_declared_fk_path",
                    "entity_grain_before_aggregate_output",
                    "outer_entity_zero_preserving",
                ],
            )

        if len(contract.ordering_requirements) == 1 \
                and not contract.aggregation_stages \
                and not contract.aggregate_requirements \
                and not contract.set_requirements \
                and not contract.distinct_count_requirements \
                and not contract.ratio_requirements \
                and not contract.correlation_requirements \
                and not contract.relationship_thresholds \
                and not contract.modifier_filters \
                and not contract.output_bundles \
                and not contract.comparison_quantifier \
                and not contract.comparison_direction:
            ordering = contract.ordering_requirements[0]
            order_by = self._plan_column_ref(ordering.get("column"))
            projections = [
                RelationalColumnRef(
                    table=str(binding.get("table") or ""),
                    column=str(binding.get("column") or ""),
                )
                for binding in contract.output_bindings
                if isinstance(binding, dict)
            ]
            typed_filters: List[RelationalFilterPredicate] = []
            for raw_filter in contract.filter_requirements:
                column = self._plan_column_ref(raw_filter.get("column"))
                if column is None or raw_filter.get("scope") != "row_predicate":
                    return None
                typed_filters.append(RelationalFilterPredicate(
                    column=column,
                    operator=str(raw_filter.get("operator") or ""),
                    value=raw_filter.get("value"),
                    value_type=str(raw_filter.get("value_type") or ""),
                ))
            physical_tables = list(dict.fromkeys([
                *(ref.table for ref in projections),
                *(item.column.table for item in typed_filters),
                *( [order_by.table] if order_by is not None else [] ),
            ]))
            paths = [
                path for path in contract.relation_paths
                if path.get("source") == "unique_shortest_declared_fk_path"
            ]
            sources: List[str] = []
            raw_edges: List[dict] = []
            if len(physical_tables) == 1:
                sources = physical_tables
            elif len(paths) == 1 and set(paths[0].get("tables") or []) \
                    == set(physical_tables):
                sources = [str(item) for item in paths[0].get("tables") or []]
                raw_edges = list(paths[0].get("edges") or [])
            joins: List[RelationalJoinEdge] = []
            for raw_edge in raw_edges:
                left = self._plan_column_ref(raw_edge.get("from"))
                right = self._plan_column_ref(raw_edge.get("to"))
                if left is None or right is None \
                        or raw_edge.get("source") != "foreign_key":
                    return None
                joins.append(RelationalJoinEdge(left=left, right=right))
            if order_by is not None and projections and sources \
                    and [ref.column for ref in projections] == contract.output_columns \
                    and ordering.get("direction") in {"ASC", "DESC"} \
                    and ordering.get("limit") == 1 \
                    and ordering.get("tie_policy") == "single_row" \
                    and contract.tie_policy == "single_row" \
                    and all(ref.table in sources for ref in projections) \
                    and order_by.table in sources \
                    and all(item.column.table in sources for item in typed_filters) \
                    and (allowed is None or all(
                        source.casefold() in allowed for source in sources
                    )):
                anchor_keys = [
                    RelationalColumnRef(sources[0], column.name)
                    for column in self.schema.tables[sources[0]].columns if column.pk
                ]
                plan_refs = [*projections, order_by, *anchor_keys]
                if anchor_keys and self._is_closed_scalar_ranking_question(
                    question, plan_refs, sources, typed_filters,
                ):
                    return RelationalScalarRankingPlan(
                        sources=sources,
                        joins=joins,
                        projections=projections,
                        filters=typed_filters,
                        order_by=order_by,
                        direction=str(ordering["direction"]),
                        limit=1,
                        tie_breakers=anchor_keys,
                        contract_version=contract.version,
                        evidence=[
                            "closed_question_shape",
                            "exact_physical_projection",
                            "schema_labeled_literal",
                            "physical_scalar_arg_extremum",
                            "deterministic_primary_key_tie_break",
                        ],
                    )
        if (
            contract.modifier_filters or contract.aggregate_requirements
            or contract.relationship_thresholds or contract.output_bundles
            or contract.set_requirements or contract.distinct_count_requirements
            or contract.ordering_requirements
            or contract.ratio_requirements or contract.correlation_requirements
            or contract.comparison_quantifier or contract.comparison_direction
        ):
            return None
        if not {
            "counted_relationship_physical_projection",
            "category_usage_physical_projection",
            "relationship_superlative_entity_identity",
        }.intersection(contract.evidence):
            return None
        if len(contract.aggregation_stages) != 2:
            return None
        group_stage, rank_stage = contract.aggregation_stages
        if (
            group_stage.get("stage") != 1
            or group_stage.get("kind") != "group_aggregate"
            or rank_stage.get("stage") != 2
            or rank_stage.get("kind") != "rank"
            or rank_stage.get("input_stage") != 1
            or rank_stage.get("direction") not in {"ASC", "DESC"}
        ):
            return None
        tie_policy = contract.tie_policy
        if tie_policy not in {"single_row", "all_ties"}:
            return None
        expected_limit = 1 if tie_policy == "single_row" else None
        if rank_stage.get("limit") != expected_limit:
            return None
        aggregates = group_stage.get("aggregates")
        if not isinstance(aggregates, list) or len(aggregates) != 1:
            return None
        aggregate_spec = aggregates[0]
        if not isinstance(aggregate_spec, dict) or (
            str(aggregate_spec.get("function") or "").upper() != "COUNT"
            or aggregate_spec.get("column") != "*"
        ):
            return None
        fact_table = str(aggregate_spec.get("source_table") or "")
        paths = [
            path for path in contract.relation_paths
            if path.get("source") == "unique_shortest_declared_fk_path"
        ]
        if len(paths) != 1:
            return None
        path = paths[0]
        sources = [str(item) for item in path.get("tables") or []]
        raw_edges = path.get("edges") or []
        if (
            len(sources) < 2 or fact_table != sources[-1]
            or len(raw_edges) != len(sources) - 1
            or any(name not in self.schema.tables for name in sources)
        ):
            return None
        if allowed is not None:
            if any(name.casefold() not in allowed for name in sources):
                return None
        group_keys = [
            self._plan_column_ref(item)
            for item in group_stage.get("group_keys") or []
        ]
        if not group_keys or any(ref is None for ref in group_keys):
            return None
        typed_group_keys = [ref for ref in group_keys if ref is not None]
        if [
            f"{ref.table}.{ref.column}" for ref in typed_group_keys
        ] != list(contract.grouping_keys):
            return None
        projections: List[RelationalColumnRef] = []
        for binding in contract.output_bindings:
            if not isinstance(binding, dict):
                return None
            ref = RelationalColumnRef(
                table=str(binding.get("table") or ""),
                column=str(binding.get("column") or ""),
            )
            if not ref.table or not ref.column:
                return None
            projections.append(ref)
        if (
            not projections
            or [ref.column for ref in projections] != list(contract.output_columns)
            or any(ref.table != sources[0] for ref in projections)
            or any(ref.table != sources[0] for ref in typed_group_keys)
        ):
            return None
        typed_filters: List[RelationalFilterPredicate] = []
        for raw_filter in contract.filter_requirements:
            if raw_filter.get("scope") != "row_predicate":
                return None
            column = self._plan_column_ref(raw_filter.get("column"))
            if column is None or column.table not in sources:
                return None
            typed_filters.append(RelationalFilterPredicate(
                column=column,
                operator=str(raw_filter.get("operator") or ""),
                value=raw_filter.get("value"),
                value_type=str(raw_filter.get("value_type") or ""),
            ))
        direction = str(rank_stage.get("direction") or "")
        joins: List[RelationalJoinEdge] = []
        for raw_edge in raw_edges:
            if not isinstance(raw_edge, dict) or raw_edge.get("source") != "foreign_key":
                return None
            left = self._plan_column_ref(raw_edge.get("from"))
            right = self._plan_column_ref(raw_edge.get("to"))
            if left is None or right is None:
                return None
            joins.append(RelationalJoinEdge(
                left=left,
                right=right,
                join_type="LEFT" if direction == "ASC" else "INNER",
            ))
        aggregate_column = "*"
        if direction == "ASC":
            final_edge = joins[-1]
            fact_endpoints = [
                ref for ref in (final_edge.left, final_edge.right)
                if ref.table == fact_table
            ]
            if len(fact_endpoints) != 1:
                return None
            aggregate_column = fact_endpoints[0].column
        plan_refs = [*projections, *typed_group_keys]
        if not self._is_closed_counted_relationship_question(
            question, plan_refs, sources, typed_filters, contract.evidence,
        ):
            return None
        return RelationalQueryPlan(
            sources=sources,
            joins=joins,
            projections=projections,
            group_keys=typed_group_keys,
            aggregate=RelationalAggregate(
                function="COUNT", source_table=fact_table, column=aggregate_column,
            ),
            ranking=RelationalRanking(
                direction=direction, tie_policy=tie_policy, limit=expected_limit,
            ),
            contract_version=contract.version,
            filters=typed_filters,
            evidence=[
                "closed_question_shape",
                "exact_physical_projection",
                "unique_declared_fk_path",
                "aggregate_before_rank",
                *(["outer_entity_zero_preserving"] if direction == "ASC" else []),
                *(["typed_scalar_filters"] if typed_filters else []),
            ],
        )

    def _sql_source_aliases(self, sql: str) -> Dict[str, str]:
        """Resolve simple physical FROM/JOIN aliases across bounded SQL shapes."""
        code = _sql_code_only(sql, mask_identifiers=False)
        canonical = {name.casefold(): name for name in self.schema.tables}
        aliases: Dict[str, str] = {}
        keywords = {
            "cross", "full", "group", "having", "inner", "join", "left", "limit",
            "on", "order", "right", "where", "union", "intersect", "except",
        }
        pattern = re.compile(
            r"\b(?:FROM|JOIN)\s+(?:ONLY\s+)?"
            r"(?:\"(?P<double>[^\"]+)\"|`(?P<backtick>[^`]+)`|"
            r"\[(?P<bracket>[^\]]+)\]|(?P<plain>[A-Za-z_][\w$]*))"
            r"(?:\s+(?:AS\s+)?(?P<alias>(?!(?:CROSS|EXCEPT|FETCH|FULL|GROUP|"
            r"HAVING|INNER|INTERSECT|JOIN|LEFT|LIMIT|ON|ORDER|RIGHT|UNION|WHERE)\b)"
            r"[A-Za-z_][\w$]*))?",
            re.IGNORECASE,
        )
        for source in pattern.finditer(code):
            raw = next(
                source.group(name) for name in ("double", "backtick", "bracket", "plain")
                if source.group(name) is not None
            )
            table_name = canonical.get(raw.casefold())
            if not table_name:
                continue
            aliases[raw.casefold()] = table_name
            alias = source.group("alias")
            if alias and alias.casefold() not in keywords:
                aliases[alias.casefold()] = table_name
        return aliases

    def _qualified_equality_edges(self, sql: str) -> set[tuple[str, str, str, str]]:
        """Extract auditable qualified column equalities with physical endpoints."""
        code = _sql_code_only(sql, mask_identifiers=False)
        aliases = self._sql_source_aliases(code)
        identifier = r'(?:"([^\"]+)"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))'
        pattern = re.compile(
            rf"(?P<left_alias>[A-Za-z_][\w$]*)\s*\.\s*{identifier}\s*=\s*"
            rf"(?P<right_alias>[A-Za-z_][\w$]*)\s*\.\s*{identifier}",
            re.IGNORECASE,
        )
        edges: set[tuple[str, str, str, str]] = set()
        for match in pattern.finditer(code):
            left_table = aliases.get(match.group("left_alias").casefold())
            right_table = aliases.get(match.group("right_alias").casefold())
            if not left_table or not right_table or left_table == right_table:
                continue
            groups = match.groups()
            # Named aliases occupy groups 0 and 5; identifier alternatives are
            # groups 1..4 and 6..9 respectively.
            left_column = next(value for value in groups[1:5] if value is not None)
            right_column = next(value for value in groups[6:10] if value is not None)
            edges.add((
                left_table.casefold(), left_column.casefold(),
                right_table.casefold(), right_column.casefold(),
            ))
        return edges

    @staticmethod
    def _outer_select_projection(sql: str) -> str:
        """Return the outer SELECT list without being fooled by scalar subquery FROM."""
        code = _sql_code_only(sql, mask_identifiers=False)
        if re.match(r"\s*WITH\b", code, re.IGNORECASE):
            return ""
        selected = re.search(r"\bSELECT\b", code, re.IGNORECASE)
        if selected is None:
            return ""
        start = selected.end()
        depth = 0
        quote = ""
        index = start
        while index < len(code):
            char = code[index]
            if quote:
                if char == quote:
                    if index + 1 < len(code) and code[index + 1] == quote:
                        index += 1
                    else:
                        quote = ""
            elif char in "'\"`":
                quote = char
            elif char == "[":
                quote = "]"
            elif char == "(":
                depth += 1
            elif char == ")" and depth:
                depth -= 1
            elif depth == 0 and code[index:index + 4].casefold() == "from" and (
                index == 0 or not (code[index - 1].isalnum() or code[index - 1] == "_")
            ) and (
                index + 4 >= len(code)
                or not (code[index + 4].isalnum() or code[index + 4] == "_")
            ):
                return code[start:index].strip()
            index += 1
        return code[start:].strip()

    @staticmethod
    def _division_denominator(projection: str) -> str:
        """Return the RHS of the shallowest arithmetic division in a projection."""
        candidates: List[tuple[int, int]] = []
        depth = 0
        quote = ""
        index = 0
        while index < len(projection):
            char = projection[index]
            if quote:
                if char == quote:
                    if index + 1 < len(projection) and projection[index + 1] == quote:
                        index += 1
                    else:
                        quote = ""
            elif char in "'\"`":
                quote = char
            elif char == "[":
                quote = "]"
            elif char == "(":
                depth += 1
            elif char == ")" and depth:
                depth -= 1
            elif char == "/":
                candidates.append((depth, index))
            index += 1
        if not candidates:
            return ""
        _depth, position = min(candidates)
        return projection[position + 1:].strip()

    def _fragment_source_tables(self, sql_fragment: str) -> List[str]:
        canonical = {name.casefold(): name for name in self.schema.tables}
        found: List[str] = []
        for match in self._SQL_SOURCE_RE.finditer(sql_fragment):
            raw = next(value for value in match.groups() if value is not None)
            table_name = canonical.get(raw.casefold())
            if table_name and table_name not in found:
                found.append(table_name)
        return found

    def _relational_algebra_retry_hint(
        self,
        question: str,
        sql: str,
        contract: Optional[RelationalAlgebraContract] = None,
    ) -> str:
        """Validate locally compiled relational operators; never rewrite SQL."""
        contract = contract or self._compile_relational_contract(question)
        if contract.ambiguities:
            ambiguity = contract.ambiguities[0]
            if ambiguity.get("kind") == "boolean_modifier_scope":
                values = ambiguity.get("category_values") or []
                rendered_values = " 或 ".join(str(value) for value in values)
                return (
                    f"问句中 {rendered_values} 后面的条件“{ambiguity.get('modifier')}”"
                    "存在布尔作用域歧义：它可能只修饰后一类，也可能同时修饰两类。"
                    "在用户明确选择作用域前，不应生成或执行 SQL。"
                )
            if ambiguity.get("kind") in {
                "result_grain_conflict", "result_grain_output_owner_conflict",
            }:
                return (
                    "本地结果形状合同发现输出实体、身份键或可见列的物理所有者不一致。"
                    "在 schema 证据收敛为同一个结果实体前，不能执行候选 SQL。"
                )
            return "问句存在尚未澄清的关系语义歧义；在用户确认口径前不应执行 SQL。"
        code = _sql_code_only(sql, mask_identifiers=False)
        predicate_sql = _sql_code_only(
            sql, mask_identifiers=False, mask_literals=False,
        )
        projection_columns = set(self._simple_projection_columns(sql))
        projection_text = self._top_level_projection(sql)
        projection_items = (
            self._split_projection(projection_text) if projection_text is not None else []
        )

        observed_edges = self._qualified_equality_edges(code)
        referenced_tables = self._sql_referenced_tables(code)

        if contract.value_domain_requirements:
            aliases = self._sql_source_aliases(code)

            def unquote_identifier(value: str) -> str:
                raw = str(value or "").strip()
                if len(raw) >= 2 and (
                    (raw[0], raw[-1]) in {
                        ('"', '"'), ('`', '`'), ('[', ']'),
                    }
                ):
                    return raw[1:-1]
                return raw

            cast_pattern = re.compile(
                r"\b(?P<function>CAST|TRY_CAST|SAFE_CAST)\s*\(\s*"
                r"(?:(?P<qualifier>\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|"
                r"[A-Za-z_][\w$]*)\s*\.\s*)?"
                r"(?P<column>\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|"
                r"[A-Za-z_][\w$]*)\s+AS\s+"
                r"(?P<target>REAL|FLOAT|DOUBLE|DECIMAL|NUMERIC|NUMBER|"
                r"INTEGER|INT|BIGINT)(?:\s*\([^)]*\))?\s*\)",
                re.IGNORECASE | re.DOTALL,
            )
            cast_calls = list(cast_pattern.finditer(predicate_sql))

            for requirement in contract.value_domain_requirements:
                qualified = str(requirement.get("column") or "")
                if qualified.count(".") != 1:
                    return "数值域合同缺少唯一物理字段，不能安全执行候选 SQL。"
                table_name, column_name = qualified.split(".", 1)
                table_folded = table_name.casefold()
                column_folded = column_name.casefold()

                def cast_matches_requirement(match: re.Match) -> bool:
                    observed_column = unquote_identifier(
                        match.group("column")
                    ).casefold()
                    if observed_column != column_folded:
                        return False
                    qualifier = unquote_identifier(
                        match.group("qualifier") or ""
                    ).casefold()
                    if qualifier:
                        physical = aliases.get(qualifier, qualifier)
                        return str(physical).casefold() == table_folded
                    owning_tables = [
                        name for name in referenced_tables
                        if name in self.schema.tables and any(
                            item.name.casefold() == column_folded
                            for item in self.schema.tables[name].columns
                        )
                    ]
                    return len(owning_tables) == 1 \
                        and owning_tables[0].casefold() == table_folded

                target_casts = [
                    item for item in cast_calls if cast_matches_requirement(item)
                ]
                coercion = str(requirement.get("coercion") or "")
                if coercion == "native_numeric" and target_casts:
                    return (
                        f"字段 {qualified} 的物理类型已经是数值；当前 CAST 会擅自改变"
                        "类型、精度或跨数据库比较语义。请直接使用原生数值列。"
                    )
                if coercion != "controlled_numeric_parse":
                    continue
                if not target_casts:
                    return (
                        f"问句把文本存储字段 {qualified} 用作数值，但候选 SQL 没有显式、"
                        "可审计的数值转换。请按当前数据库方言转换，并排除无效文本。"
                    )

                qualifier_names = [
                    alias for alias, physical in aliases.items()
                    if str(physical).casefold() == table_folded
                ]
                qualifier_names.append(table_name)
                qualifier_pattern = "|".join(
                    re.escape(item) for item in dict.fromkeys(qualifier_names)
                    if item
                )
                identifier_pattern = (
                    rf'(?:"{re.escape(column_name)}"|`{re.escape(column_name)}`|'
                    rf'\[{re.escape(column_name)}\]|{re.escape(column_name)})'
                )
                reference_pattern = (
                    rf'(?:(?:{qualifier_pattern})\s*\.\s*)?{identifier_pattern}'
                    if qualifier_pattern else identifier_pattern
                )
                validity_guard = bool(
                    re.search(
                        rf"{reference_pattern}\s+(?:NOT\s+)?(?:GLOB|REGEXP|RLIKE|"
                        rf"SIMILAR\s+TO|~)\b|"
                        rf"\bREGEXP_LIKE\s*\(\s*{reference_pattern}\b|"
                        rf"\b(?:TRY_CAST|SAFE_CAST)\s*\([^)]*{reference_pattern}"
                        rf"[^)]*\)\s+IS\s+NOT\s+NULL",
                        predicate_sql,
                        re.IGNORECASE | re.DOTALL,
                    )
                )
                if not validity_guard:
                    return (
                        f"字段 {qualified} 是文本存储的业务数值；无保护 CAST 会把无效文本"
                        "静默转换成 0 或产生方言相关结果。请增加可审计的数值格式校验，"
                        "按合同排除无效值后再聚合、比较或排序。"
                    )

        for requirement in contract.boolean_filter_requirements:
            category_column = str(
                requirement.get("category_column") or ""
            ).rsplit(".", 1)[-1]
            modifier_column = str(
                requirement.get("modifier_column") or ""
            ).rsplit(".", 1)[-1]
            values = [str(value) for value in requirement.get("category_values") or []]
            if not category_column or not modifier_column or len(values) != 2:
                return "布尔筛选合同不完整；不能在缺少物理列或类别值时执行候选 SQL。"

            def column_ref(column_name: str) -> str:
                return (
                    r"(?:(?:[A-Za-z_][\w$]*)\s*\.\s*)?"
                    rf"[`\"\[]?{re.escape(column_name)}[`\"\]]?"
                )

            def category_predicate(value: str) -> str:
                return (
                    column_ref(category_column)
                    + rf"\s*=\s*['\"]{re.escape(value)}['\"]"
                )

            left_predicate = category_predicate(values[0])
            right_predicate = category_predicate(values[1])
            modifier_predicate = (
                column_ref(modifier_column)
                + rf"\s*{re.escape(str(requirement.get('modifier_operator') or ''))}"
                + rf"\s*{re.escape(str(requirement.get('modifier_value')))}"
                + r"(?![0-9.])"
            )
            scope = str(requirement.get("scope") or "")
            satisfied = False
            if scope == "right_category_only":
                branches = re.split(
                    r"\bUNION\b(?:\s+ALL\b)?",
                    predicate_sql,
                    flags=re.IGNORECASE,
                )
                if len(branches) == 2:
                    left_branch, right_branch = branches
                    satisfied = bool(
                        re.search(left_predicate, left_branch, re.IGNORECASE)
                        and not re.search(
                            modifier_predicate, left_branch, re.IGNORECASE,
                        )
                        and re.search(right_predicate, right_branch, re.IGNORECASE)
                        and re.search(
                            modifier_predicate, right_branch, re.IGNORECASE,
                        )
                    ) or bool(
                        re.search(left_predicate, right_branch, re.IGNORECASE)
                        and not re.search(
                            modifier_predicate, right_branch, re.IGNORECASE,
                        )
                        and re.search(right_predicate, left_branch, re.IGNORECASE)
                        and re.search(
                            modifier_predicate, left_branch, re.IGNORECASE,
                        )
                    )
                right_clause = (
                    rf"(?:{right_predicate}.*?\bAND\b.*?{modifier_predicate}|"
                    rf"{modifier_predicate}.*?\bAND\b.*?{right_predicate})"
                )
                satisfied = satisfied or bool(re.search(
                    rf"(?:{left_predicate}\s+OR\s+\(?\s*{right_clause}|"
                    rf"{right_clause}\s*\)?\s+OR\s+{left_predicate})",
                    predicate_sql,
                    re.IGNORECASE | re.DOTALL,
                ))
            elif scope == "both_categories":
                in_group = (
                    column_ref(category_column)
                    + r"\s+IN\s*\(\s*"
                    + rf"['\"]{re.escape(values[0])}['\"]\s*,\s*"
                    + rf"['\"]{re.escape(values[1])}['\"]\s*\)"
                )
                or_group = (
                    rf"\(\s*(?:{left_predicate}\s+OR\s+{right_predicate}|"
                    rf"{right_predicate}\s+OR\s+{left_predicate})\s*\)"
                )
                category_group = rf"(?:{in_group}|{or_group})"
                satisfied = bool(re.search(
                    rf"(?:{category_group}\s+AND\s+{modifier_predicate}|"
                    rf"{modifier_predicate}\s+AND\s+{category_group})",
                    predicate_sql,
                    re.IGNORECASE | re.DOTALL,
                ))
            if not satisfied:
                scope_label = (
                    "只作用于后一类别" if scope == "right_category_only"
                    else "同时作用于两个类别"
                )
                return (
                    f"用户已确认布尔筛选条件应{scope_label}，但候选 SQL 没有按该作用域"
                    f"组合 {category_column} 的两个值与 {modifier_column} 条件。"
                )

        def correlated_not_exists_edge(
            left_table: str, left_column: str,
            right_table: str, right_column: str,
        ) -> bool:
            """Recognize a bounded FK anti-semijoin with an outer key binding.

            The inner FK may be unqualified because SQL resolves it against the
            NOT EXISTS subquery's sole physical FROM source.  The outer key must
            remain qualified, preventing an uncorrelated global emptiness test
            from being accepted as a per-entity anti-join.
            """
            aliases = self._sql_source_aliases(code)

            def identifier(value: str) -> str:
                return rf'(?:"{re.escape(value)}"|`{re.escape(value)}`|' \
                    rf'\[{re.escape(value)}\]|{re.escape(value)})'

            for inner_table, inner_column, outer_table, outer_column in (
                (left_table, left_column, right_table, right_column),
                (right_table, right_column, left_table, left_column),
            ):
                outer_qualifiers = [
                    alias for alias, physical in aliases.items()
                    if physical.casefold() == outer_table.casefold()
                ]
                inner_qualifiers = [
                    alias for alias, physical in aliases.items()
                    if physical.casefold() == inner_table.casefold()
                ]
                if not outer_qualifiers:
                    continue
                outer_ref = (
                    rf'(?:{"|".join(re.escape(item) for item in outer_qualifiers)})'
                    rf'\s*\.\s*{identifier(outer_column)}'
                )
                inner_prefix = (
                    rf'(?:(?:{"|".join(re.escape(item) for item in inner_qualifiers)})'
                    rf'\s*\.\s*)?'
                ) if inner_qualifiers else ""
                inner_ref = inner_prefix + identifier(inner_column)
                equality = rf'(?:{inner_ref}\s*=\s*{outer_ref}|{outer_ref}\s*=\s*{inner_ref})'
                if re.search(
                    rf'\bNOT\s+EXISTS\s*\([^)]*\bFROM\s+'
                    rf'{identifier(inner_table)}(?![A-Za-z0-9_])[^)]*{equality}[^)]*\)',
                    code,
                    re.IGNORECASE | re.DOTALL,
                ):
                    return True
            return False

        for path in contract.relation_paths:
            for edge in path.get("edges") or []:
                left = str(edge.get("from") or "")
                right = str(edge.get("to") or "")
                if "." not in left or "." not in right:
                    continue
                left_table, left_column = left.rsplit(".", 1)
                right_table, right_column = right.rsplit(".", 1)
                if path.get("enforcement") == "when_both_tables_referenced" and not {
                    left_table.casefold(), right_table.casefold(),
                }.issubset({item.casefold() for item in referenced_tables}):
                    continue
                expected = (
                    left_table.casefold(), left_column.casefold(),
                    right_table.casefold(), right_column.casefold(),
                )
                reverse = (expected[2], expected[3], expected[0], expected[1])
                using_equivalent = bool(
                    left_column.casefold() == right_column.casefold()
                    and {left_table, right_table}.issubset(set(referenced_tables))
                    and re.search(
                        rf"\bUSING\s*\(\s*[`\"\[]?{re.escape(left_column)}"
                        rf"[`\"\]]?\s*\)",
                        code,
                        re.IGNORECASE,
                    )
                )
                membership_equivalent = any(re.search(
                    rf"(?:[A-Za-z_][\w$]*\s*\.\s*)?[`\"\[]?"
                    rf"{re.escape(outer_column)}[`\"\]]?\s+IN\s*\(\s*"
                    rf"SELECT\s+(?:DISTINCT\s+)?(?:[A-Za-z_][\w$]*\s*\.\s*)?"
                    rf"[`\"\[]?{re.escape(inner_column)}[`\"\]]?\s+FROM\s+"
                    rf"[`\"\[]?{re.escape(inner_table)}[`\"\]]?",
                    code,
                    re.IGNORECASE | re.DOTALL,
                ) for outer_column, inner_column, inner_table in (
                    (left_column, right_column, right_table),
                    (right_column, left_column, left_table),
                ))
                if expected not in observed_edges and reverse not in observed_edges \
                        and not using_equivalent and not membership_equivalent \
                        and not correlated_not_exists_edge(
                            left_table, left_column, right_table, right_column,
                        ):
                    return (
                        "本地关系路径合同要求实际关联边 " + left + " = " + right
                        + "，但候选 SQL 没有证明这条边。表集合可连通不等于所选 JOIN 列正确；"
                        "请使用该唯一已声明外键路径，或提供用户明确关系证据。"
                    )

        for ratio in contract.ratio_requirements:
            projection = self._outer_select_projection(code)
            denominator = self._division_denominator(projection)
            implicit_average = bool(
                not denominator
                and re.search(r"\bAVG\s*\(\s*(?:CASE\b|IIF\s*\()", projection, re.IGNORECASE)
            )
            if not denominator and not implicit_average:
                return (
                    "问句要求比例/百分比，但候选投影没有可审计的除法分母或等价条件 AVG。"
                    "请显式保留分子与总体分母，避免只返回达标数量。"
                )
            denominator_scope = self._fragment_source_tables(denominator) \
                if denominator and re.search(r"\bSELECT\b", denominator, re.IGNORECASE) \
                else referenced_tables
            required_scope = [str(item) for item in ratio.get("population_tables") or []]
            missing_scope = [
                table for table in required_scope
                if table.casefold() not in {item.casefold() for item in denominator_scope}
            ]
            if missing_scope:
                return (
                    "比例分母改变了总体关系范围：本地合同要求分母保持 "
                    + "、".join(required_scope)
                    + "，当前分母没有包含 " + "、".join(missing_scope)
                    + "。分子条件可以更窄，但分母不能切换到另一张维表或丢失基础关联。"
                )
            predicate_target = denominator if denominator and re.search(
                r"\bSELECT\b", denominator, re.IGNORECASE,
            ) else code
            predicate_sections = re.findall(
                r"\b(?:WHERE|HAVING|ON)\b(?P<body>.*?)(?=\bGROUP\s+BY\b|"
                r"\bORDER\s+BY\b|\bLIMIT\b|\bUNION\b|\bINTERSECT\b|"
                r"\bEXCEPT\b|$)",
                predicate_target,
                re.IGNORECASE | re.DOTALL,
            )
            missing_base_filters = []
            for qualified in ratio.get("base_filter_columns") or []:
                column_name = str(qualified).rsplit(".", 1)[-1]
                if not any(re.search(
                    rf"(?<![A-Za-z0-9_]){re.escape(column_name)}(?![A-Za-z0-9_])",
                    section,
                    re.IGNORECASE,
                ) for section in predicate_sections):
                    missing_base_filters.append(str(qualified))
            if missing_base_filters:
                return (
                    "比例分母没有继承总体的基础筛选列 "
                    + "、".join(missing_base_filters)
                    + "。达标条件只属于分子，但日期、分群等总体口径必须同时约束分母。"
                )
            grain = str(ratio.get("denominator_grain") or "")
            if grain and denominator and not implicit_average:
                grain_column = grain.rsplit(".", 1)[-1]
                count_match = re.search(
                    r"\bCOUNT\s*\(\s*(?:DISTINCT\s+)?"
                    r"(?P<column>\*|(?:(?:[A-Za-z_][\w$]*)\s*\.\s*)?"
                    r"(?:\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*))",
                    denominator,
                    re.IGNORECASE,
                )
                if count_match:
                    observed_column = count_match.group("column").split(".")[-1].strip(
                        '"`[] '
                    )
                    if observed_column != "*" and observed_column.casefold() \
                            != grain_column.casefold():
                        return (
                            f"比例分母的实体粒度应为 {grain}，但当前 COUNT 使用 "
                            f"{observed_column}。请让分子与分母统计同一实体粒度。"
                        )
            if int(ratio.get("scale") or 1) == 100 and not re.search(
                r"(?:\b100(?:\.0+)?\s*\*|\*\s*100(?:\.0+)?\b)",
                projection,
                re.IGNORECASE,
            ):
                return "问句要求百分比，但候选比例没有乘以 100；请返回百分数而不是 0–1 小数。"

        for correlation in contract.correlation_requirements:
            path = correlation.get("path") or {}
            path_edges = path.get("edges") or []
            edge_satisfied = True
            for edge in path_edges:
                left = str(edge.get("from") or "")
                right = str(edge.get("to") or "")
                if "." not in left or "." not in right:
                    continue
                left_table, left_column = left.rsplit(".", 1)
                right_table, right_column = right.rsplit(".", 1)
                expected = (
                    left_table.casefold(), left_column.casefold(),
                    right_table.casefold(), right_column.casefold(),
                )
                if expected not in observed_edges and (
                    expected[2], expected[3], expected[0], expected[1]
                ) not in observed_edges:
                    edge_satisfied = False
                    break

            not_exists = bool(re.search(r"\bNOT\s+EXISTS\s*\(", code, re.IGNORECASE))
            null_reject = bool(re.search(
                r"\bLEFT\s+JOIN\b.*?\bIS\s+NULL\b", code, re.IGNORECASE | re.DOTALL,
            ))
            not_in_satisfied = False
            if len(path_edges) == 1:
                edge = path_edges[0]
                left = str(edge.get("from") or "")
                right = str(edge.get("to") or "")
                outer_table = str(correlation.get("outer_table") or "")
                outer_column = ""
                inner_column = ""
                if left.startswith(outer_table + "."):
                    outer_column, inner_column = left.rsplit(".", 1)[-1], right.rsplit(".", 1)[-1]
                elif right.startswith(outer_table + "."):
                    outer_column, inner_column = right.rsplit(".", 1)[-1], left.rsplit(".", 1)[-1]
                if outer_column and inner_column:
                    not_in_satisfied = bool(re.search(
                        rf"(?:[A-Za-z_][\w$]*\s*\.\s*)?[`\"\[]?"
                        rf"{re.escape(outer_column)}[`\"\]]?\s+NOT\s+IN\s*\(\s*"
                        rf"SELECT\s+(?:DISTINCT\s+)?(?:[A-Za-z_][\w$]*\s*\.\s*)?"
                        rf"[`\"\[]?{re.escape(inner_column)}[`\"\]]?\s+FROM\s+"
                        rf"[`\"\[]?{re.escape(str(correlation.get('inner_table') or ''))}"
                        rf"[`\"\]]?",
                        code,
                        re.IGNORECASE | re.DOTALL,
                    ))
            if not (
                (not_exists and edge_satisfied)
                or (null_reject and edge_satisfied)
                or not_in_satisfied
            ):
                rendered = "、".join(
                    f"{edge.get('from')} = {edge.get('to')}" for edge in path_edges
                )
                return (
                    "问句要求不存在关联记录；候选必须用绑定外层实体键的 NOT EXISTS、"
                    "等价 NOT IN 或 LEFT JOIN ... IS NULL。当前没有证明反关联绑定 "
                    + rendered + "，未相关的子查询会把全库有无记录误当成每个实体的有无记录。"
                )

        for stage in contract.aggregation_stages:
            kind = str(stage.get("kind") or "")
            if kind == "group_aggregate":
                for aggregate in stage.get("aggregates") or []:
                    function = str(aggregate.get("function") or "").upper()
                    if function and not re.search(
                        rf"\b{re.escape(function)}\s*\(", code, re.IGNORECASE,
                    ):
                        return (
                            f"聚合阶段 1 要求先计算 {function}，但候选没有该聚合。"
                            "请先把事实行归约到实体粒度，再执行后续比较或排名。"
                        )
                has_group = bool(re.search(r"\bGROUP\s+BY\b", code, re.IGNORECASE))
                correlated_reduction = bool(
                    not has_group
                    and re.search(
                        r"\(\s*SELECT\s+(?:COUNT|SUM|AVG|MIN|MAX)\s*\(",
                        code,
                        re.IGNORECASE,
                    )
                    and observed_edges
                )
                if not has_group and not correlated_reduction:
                    return (
                        "聚合阶段 1 必须先按实体粒度归约事实行；当前既没有 GROUP BY，"
                        "也没有与外层实体键绑定的相关聚合子查询。"
                    )
            elif kind == "rank":
                direction = str(stage.get("direction") or "").upper()
                has_single_limit = bool(re.search(
                    r"\b(?:LIMIT\s+1|TOP\s+1|FETCH\s+(?:FIRST|NEXT)\s+1\s+ROWS?\s+ONLY)\b",
                    code,
                    re.IGNORECASE,
                ))
                # Preserve the older, more specific tie-policy diagnostic when
                # a singular superlative omitted its row limit entirely.
                if stage.get("limit") == 1 and not has_single_limit:
                    continue
                order = re.search(
                    r"\bORDER\s+BY\b(?P<body>.*?)(?=\bLIMIT\b|\bFETCH\b|$)",
                    code,
                    re.IGNORECASE | re.DOTALL,
                )
                extremum = "MAX" if direction == "DESC" else "MIN"
                aggregate_aliases = {
                    match.group("alias").casefold()
                    for aggregate_stage in contract.aggregation_stages
                    if aggregate_stage.get("kind") == "group_aggregate"
                    for aggregate in aggregate_stage.get("aggregates") or []
                    for function in [str(aggregate.get("function") or "").upper()]
                    if function
                    for match in re.finditer(
                        rf"\b{re.escape(function)}\s*\([^)]*\)\s+AS\s+"
                        rf"[`\"\[]?(?P<alias>[A-Za-z_][\w$]*)[`\"\]]?",
                        code,
                        re.IGNORECASE | re.DOTALL,
                    )
                }
                scalar_extremum_equivalent = any(
                    match.group("value").casefold() in aggregate_aliases
                    for match in re.finditer(
                        rf"\b{extremum}\s*\(\s*(?:[A-Za-z_][\w$]*\s*\.\s*)?"
                        rf"[`\"\[]?(?P<value>[A-Za-z_][\w$]*)[`\"\]]?\s*\)",
                        code,
                        re.IGNORECASE,
                    )
                )
                if not scalar_extremum_equivalent and (
                    order is None or not re.search(
                    rf"\b{re.escape(direction)}\b", order.group("body"), re.IGNORECASE,
                    )
                ):
                    return (
                        f"聚合阶段 2 要求在实体聚合结果上按 {direction} 排名，"
                        "但候选缺少对应 ORDER BY 方向。"
                    )
                if stage.get("limit") == 1 and not has_single_limit:
                    return "聚合阶段 2 要求单个实体结果，但候选排名后没有单行限制。"

        if "rank_projection" in contract.required_operators and not re.search(
            r"\b(?:RANK|DENSE_RANK|ROW_NUMBER)\s*\(.*?\)\s*OVER\s*\(",
            code, re.IGNORECASE | re.DOTALL,
        ):
            expected_outputs = (
                "、".join(contract.output_columns) + "，以及一个排名列"
                if contract.output_columns else "用户要求的业务列，以及一个排名列"
            )
            return (
                "问句明确要求给出排名结果，但当前 SQL 只排序、没有在 SELECT 中返回 "
                "RANK/DENSE_RANK/ROW_NUMBER 排名列。SELECT 应只返回"
                + expected_outputs
                + "；排序或筛选字段不要额外投影。"
            )

        if "group_by" in contract.required_operators and re.search(
            r"\b(?:COUNT|SUM|AVG|MIN|MAX)\s*\(", code, re.IGNORECASE,
        ) and not re.search(r"\bGROUP\s+BY\b", code, re.IGNORECASE):
            return "问句要求逐实体统计，但当前聚合没有 GROUP BY，会把所有实体折叠成一行。请按实体键分组。"

        group_match = re.search(
            r"\bGROUP\s+BY\b(?P<body>.*?)(?=\bHAVING\b|\bORDER\s+BY\b|"
            r"\bLIMIT\b|\bUNION\b|\bINTERSECT\b|\bEXCEPT\b|$)",
            code,
            re.IGNORECASE | re.DOTALL,
        )
        if contract.grouping_keys and group_match:
            grouped_names = {
                match.group("column").strip('"`[]').casefold()
                for match in re.finditer(
                    r"(?:(?:[A-Za-z_][\w$]*)\s*\.\s*)?"
                    r"(?P<column>\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*)",
                    group_match.group("body"),
                    re.IGNORECASE,
                )
            }
            missing_keys = [
                key for key in contract.grouping_keys
                if key.rsplit(".", 1)[-1].casefold() not in grouped_names
            ]
            if missing_keys:
                return (
                    "问句要求逐实体聚合，schema 已声明稳定实体键 "
                    + "、".join(contract.grouping_keys)
                    + "，但当前 GROUP BY 未包含这些键。描述列可以附加分组，"
                    "但不能替代实体主键，否则同名实体会被合并。"
                )

        if contract.result_grain:
            owner_table = str(contract.result_grain.get("owner_table") or "")
            identity_columns = [
                str(item) for item in (
                    contract.result_grain.get("identity_columns") or []
                )
            ]
            visible_columns = [
                str(item) for item in (
                    contract.result_grain.get("visible_columns") or []
                )
            ]
            visible_owners = {
                item.split(".", 1)[0].casefold()
                for item in visible_columns if "." in item
            }
            if owner_table and visible_owners \
                    and visible_owners != {owner_table.casefold()}:
                return (
                    f"结果实体已绑定为 {owner_table}，但候选输出来自其他物理实体。"
                    "展示列和聚合实体必须共享同一个结果所有者。"
                )
            grouped_stage = next((
                item for item in contract.aggregation_stages
                if item.get("kind") == "group_aggregate"
            ), None)
            if grouped_stage is not None and identity_columns:
                stage_keys = [
                    str(item).casefold()
                    for item in grouped_stage.get("group_keys") or []
                ]
                if stage_keys != [item.casefold() for item in identity_columns]:
                    return (
                        "关系计划的聚合阶段没有使用结果实体的身份键；"
                        "展示名称不能替代实体主键或明确类别值作为统计粒度。"
                    )

        for subject in contract.aggregate_subjects:
            function = str(subject.get("function") or "").upper()
            multiplicity = str(subject.get("multiplicity") or "")
            if function == "COUNT" and multiplicity == "fact_rows":
                count_calls = re.findall(
                    r"\bCOUNT\s*\(\s*(?P<body>[^)]*)\)",
                    predicate_sql,
                    re.IGNORECASE | re.DOTALL,
                )
                aliases = self._sql_source_aliases(code)

                def distinct_is_proven_fact_key(body: str) -> bool:
                    match = re.fullmatch(
                        r"\s*DISTINCT\s+"
                        r"(?:(?P<qualifier>[A-Za-z_][\w$]*)\s*\.\s*)?"
                        r"[`\"\[]?(?P<column>[A-Za-z_][\w$]*)[`\"\]]?\s*",
                        body,
                        re.IGNORECASE,
                    )
                    if match is None:
                        return False
                    qualifier = str(match.group("qualifier") or "").casefold()
                    source_table = str(subject.get("source_table") or "")
                    physical = aliases.get(qualifier, qualifier) if qualifier else source_table
                    table = self.schema.tables.get(physical)
                    return bool(
                        table is not None
                        and physical.casefold() == source_table.casefold()
                        and any(
                            column.pk
                            and column.name.casefold()
                            == match.group("column").casefold()
                            for column in table.columns
                        )
                    )

                if count_calls and all(
                    re.match(r"\s*DISTINCT\b", body, re.IGNORECASE)
                    and not distinct_is_proven_fact_key(body)
                    for body in count_calls
                ):
                    return (
                        "聚合主体是关联事实行，候选却只使用 COUNT(DISTINCT ...)。"
                        "事实行数、唯一实体数和数值总和是不同统计主体，不能互换。"
                    )

        if contract.aggregate_requirements:
            aggregate_facts: List[tuple[str, str]] = []
            for aggregate in re.finditer(
                r"\b(?P<function>AVG|SUM|MIN|MAX|COUNT)\s*\(\s*"
                r"(?:DISTINCT\s+)?(?:CAST\s*\(\s*)?"
                r"(?:(?:[A-Za-z_][\w$]*)\s*\.\s*)?"
                r"(?P<column>\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*)",
                predicate_sql,
                re.IGNORECASE,
            ):
                aggregate_facts.append((
                    aggregate.group("function").upper(),
                    aggregate.group("column").strip('"`[]').casefold(),
                ))
            for requirement in contract.aggregate_requirements:
                function = str(requirement.get("function") or "").upper()
                qualified = str(requirement.get("column") or "")
                column_name = qualified.rsplit(".", 1)[-1].casefold()
                if (function, column_name) not in aggregate_facts:
                    return (
                        f"问句明确要求 {function} 聚合 {qualified}，但候选 SQL 的聚合函数/"
                        "输入列不匹配。请让聚合输入绑定到该物理列；筛选列或实体键不能替代它。"
                    )

        if "having" in contract.required_operators and not (
            re.search(r"\bHAVING\b", code, re.IGNORECASE)
            or re.search(r"\b(?:INTERSECT|EXISTS)\b", code, re.IGNORECASE)
        ):
            return "问句按关联记录数量筛选实体，但当前没有 HAVING/等价集合约束。请先按实体分组，再筛选聚合数量。"

        for requirement in contract.filter_requirements:
            qualified = str(requirement.get("column") or "")
            column_name = qualified.rsplit(".", 1)[-1]
            operator = str(requirement.get("operator") or "")
            value = requirement.get("value")
            if requirement.get("value_type") == "number":
                literal = re.escape(str(value))
            else:
                literal = rf"['\"]{re.escape(str(value))}['\"]"
            if not re.search(
                rf"(?<![A-Za-z0-9_$])(?:[A-Za-z_][\w$]*\s*\.\s*)?"
                rf"[`\"\[]?{re.escape(column_name)}[`\"\]]?"
                rf"(?![A-Za-z0-9_$])\s*{re.escape(operator)}\s*{literal}(?![0-9.])",
                predicate_sql,
                re.IGNORECASE,
            ):
                return (
                    f"问句的类型化过滤要求 {qualified} {operator} {value} "
                    "没有在候选 SQL 中按物理列和比较值实现。"
                )

        for policy in contract.predicate_literal_policies:
            if policy.get("mode") != "question_grounded_string_predicates":
                continue
            allowed_values = {
                re.sub(r"\s+", " ", str(value)).strip().casefold()
                for value in policy.get("allowed_values") or []
            }
            typed_values = {
                re.sub(r"\s+", " ", str(item.get("value") or "")).strip().casefold()
                for item in contract.filter_requirements
                if item.get("value") is not None
            }
            modifier_columns = {
                str(value).rsplit(".", 1)[-1].casefold()
                for value in contract.modifier_filters
            }

            def mentioned_by_question(value: str) -> bool:
                normalized = re.sub(r"[%_]", "", value).strip()
                if not normalized:
                    return True
                return bool(re.search(
                    rf"(?<![A-Za-z0-9]){re.escape(normalized)}"
                    rf"(?![A-Za-z0-9])",
                    question,
                    re.IGNORECASE,
                ))

            ungrounded: List[tuple[str, str]] = []
            literal_predicate = re.compile(
                r"(?<![A-Za-z0-9_$])"
                r"(?:(?:[A-Za-z_][\w$]*)\s*\.\s*)?"
                r"[`\"\[]?(?P<column>[A-Za-z_][\w$]*)[`\"\]]?"
                r"\s*(?:=|<>|!=|LIKE\b)\s*"
                r"(?P<quote>['\"])(?P<literal>.*?)(?P=quote)",
                re.IGNORECASE | re.DOTALL,
            )
            for match in literal_predicate.finditer(predicate_sql):
                column_name = match.group("column").casefold()
                literal = re.sub(
                    r"\s+", " ", match.group("literal")
                ).strip()
                folded_literal = re.sub(r"[%_]", "", literal).strip().casefold()
                if column_name in modifier_columns or folded_literal in allowed_values \
                        or folded_literal in typed_values \
                        or mentioned_by_question(literal):
                    continue
                item = (match.group("column"), literal)
                if item not in ungrounded:
                    ungrounded.append(item)
            in_predicate = re.compile(
                r"(?<![A-Za-z0-9_$])"
                r"(?:(?:[A-Za-z_][\w$]*)\s*\.\s*)?"
                r"[`\"\[]?(?P<column>[A-Za-z_][\w$]*)[`\"\]]?"
                r"\s+IN\s*\((?P<body>[^()]*)\)",
                re.IGNORECASE | re.DOTALL,
            )
            for predicate in in_predicate.finditer(predicate_sql):
                column_name = predicate.group("column").casefold()
                for literal_match in re.finditer(
                    r"(?P<quote>['\"])(?P<literal>.*?)(?P=quote)",
                    predicate.group("body"),
                    re.DOTALL,
                ):
                    literal = re.sub(
                        r"\s+", " ", literal_match.group("literal")
                    ).strip()
                    folded_literal = re.sub(
                        r"[%_]", "", literal
                    ).strip().casefold()
                    if column_name in modifier_columns \
                            or folded_literal in allowed_values \
                            or folded_literal in typed_values \
                            or mentioned_by_question(literal):
                        continue
                    item = (predicate.group("column"), literal)
                    if item not in ungrounded:
                        ungrounded.append(item)
            if ungrounded:
                rendered = "、".join(
                    f"{column}={literal!r}" for column, literal in ungrounded
                )
                return (
                    "候选 SQL 添加了问句、类型化筛选和已声明修饰词均未提供依据的"
                    f"字符串条件：{rendered}。请删除无来源谓词；schema 中存在某个值"
                    "并不等于用户要求用它筛选。"
                )

        for requirement in contract.distinct_row_requirements:
            columns = [
                str(value).rsplit(".", 1)[-1]
                for value in requirement.get("columns") or []
            ]
            if not columns:
                continue
            outer_distinct = bool(re.match(
                r"\s*SELECT\s+DISTINCT\b", code, re.IGNORECASE,
            ))
            distinct_set = bool(
                re.search(r"\bUNION\b(?!\s+ALL\b)", code, re.IGNORECASE)
                or re.search(r"\bINTERSECT\b|\bEXCEPT\b", code, re.IGNORECASE)
            )
            group_match = re.search(
                r"\bGROUP\s+BY\b(?P<body>.*?)(?=\bHAVING\b|\bORDER\s+BY\b|"
                r"\bLIMIT\b|\bUNION\b|\bINTERSECT\b|\bEXCEPT\b|$)",
                code,
                re.IGNORECASE | re.DOTALL,
            )
            grouped_names = {
                match.group("column").strip('"`[]').casefold()
                for match in re.finditer(
                    r"(?:(?:[A-Za-z_][\w$]*)\s*\.\s*)?"
                    r"(?P<column>\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|"
                    r"[A-Za-z_][\w$]*)",
                    group_match.group("body") if group_match else "",
                    re.IGNORECASE,
                )
            }
            grouped_tuple = bool(
                grouped_names
                and {column.casefold() for column in columns}.issubset(grouped_names)
            )
            if not (outer_distinct or distinct_set or grouped_tuple):
                return (
                    "问句要求每个实体对应的描述组合，重复事实行不能重复输出；"
                    "候选必须使用 SELECT DISTINCT、等价集合运算，或按完整输出组合分组："
                    + "、".join(columns) + "。"
                )

        for requirement in contract.ordering_requirements:
            qualified = str(requirement.get("column") or "")
            column_name = qualified.rsplit(".", 1)[-1]
            direction = str(requirement.get("direction") or "").upper()
            order = re.search(
                r"\bORDER\s+BY\b(?P<body>.*?)(?=\bLIMIT\b|\bFETCH\b|$)",
                code,
                re.IGNORECASE | re.DOTALL,
            )
            ordered_scalar = bool(
                order
                and re.search(
                    rf"(?<![A-Za-z0-9_$])(?:[A-Za-z_][\w$]*\s*\.\s*)?"
                    rf"[`\"\[]?{re.escape(column_name)}[`\"\]]?"
                    rf"(?![A-Za-z0-9_$])\s+{re.escape(direction)}\b",
                    order.group("body"),
                    re.IGNORECASE,
                )
            )
            scalar_extremum = "MIN" if direction == "ASC" else "MAX"
            extremum_equivalent = bool(re.search(
                rf"\b{scalar_extremum}\s*\(\s*"
                rf"(?:[A-Za-z_][\w$]*\s*\.\s*)?[`\"\[]?"
                rf"{re.escape(column_name)}[`\"\]]?\s*\)",
                code,
                re.IGNORECASE,
            ))
            has_single_limit = bool(re.search(
                r"\b(?:LIMIT\s+1|TOP\s+1|FETCH\s+(?:FIRST|NEXT)\s+1\s+ROWS?\s+ONLY)\b",
                code,
                re.IGNORECASE,
            ))
            if not ordered_scalar and not extremum_equivalent:
                return (
                    f"问句要求按物理标量 {qualified} {direction} 选择极值实体，"
                    "但候选没有绑定该排序列或等价 MIN/MAX。"
                )
            if int(requirement.get("limit") or 0) == 1 \
                    and ordered_scalar and not has_single_limit:
                return "问句要求一个确定的标量极值实体，但排序后没有单行限制。"

        for requirement in contract.set_requirements:
            operator = str(requirement.get("operator") or "").upper()
            branches = requirement.get("branches") or []
            branch_tables = {
                str(item.get("table") or "").casefold() for item in branches
            }
            referenced = {name.casefold() for name in self._sql_referenced_tables(sql)}
            if operator == "INTERSECT":
                uses_intersection = bool(
                    branch_tables.issubset(referenced)
                    and (
                        re.search(r"\bINTERSECT\b", code, re.IGNORECASE)
                        or re.search(
                            r"\bIN\s*\(\s*SELECT\b|\bEXISTS\s*\(\s*SELECT\b",
                            code,
                            re.IGNORECASE | re.DOTALL,
                        )
                    )
                )
                if not uses_intersection:
                    return (
                        "问句要求两个独立实体集合的公共值，必须用 INTERSECT、"
                        "绑定同一输出列的 IN 子查询或相关 EXISTS 表达交集；"
                        "沿其他业务事实表 JOIN 只会得到该关联子集。"
                    )
            elif operator == "EXCEPT":
                proof = requirement.get("proof_edge") or {}
                left = str(proof.get("from") or "")
                right = str(proof.get("to") or "")
                correlated_equivalent = False
                if "." in left and "." in right:
                    left_table, left_column = left.rsplit(".", 1)
                    right_table, right_column = right.rsplit(".", 1)
                    correlated_equivalent = correlated_not_exists_edge(
                        left_table, left_column, right_table, right_column,
                    )
                if not (
                    branch_tables.issubset(referenced)
                    and (
                        re.search(r"\bEXCEPT\b", code, re.IGNORECASE)
                        or correlated_equivalent
                    )
                ):
                    return (
                        "问句要求主实体键排除已出现在关联事实中的键；"
                        "当前未用两个 schema 绑定键的 EXCEPT 或绑定外层键的 "
                        "NOT EXISTS 实现该反集合。"
                    )
            elif operator == "ALL_VALUES":
                values = [str(value) for value in requirement.get("values") or []]
                value_column = str(requirement.get("value_column") or "").rsplit(".", 1)[-1]
                relation_path = requirement.get("relation_path") or {}
                required_tables = {
                    str(table).casefold()
                    for table in relation_path.get("tables") or []
                    if table
                } or {
                    str(requirement.get("fact_table") or "").casefold(),
                    str(requirement.get("parent_table") or "").casefold(),
                }
                literals_present = all(re.search(
                    rf"['\"]{re.escape(value)}['\"]",
                    predicate_sql,
                    re.IGNORECASE,
                ) for value in values)
                uses_intersection = bool(re.search(r"\bINTERSECT\b", code, re.IGNORECASE))
                uses_two_exists = len(re.findall(
                    r"\bEXISTS\s*\(", code, re.IGNORECASE,
                )) >= len(values)
                value_table_name = str(requirement.get("fact_table") or "")
                countable_columns = [value_column]
                value_table_schema = self.schema.tables.get(value_table_name)
                if value_table_schema is not None:
                    countable_columns.extend(
                        column.name for column in value_table_schema.columns
                        if column.pk
                    )
                uses_grouped_all = bool(
                    re.search(r"\bHAVING\b", code, re.IGNORECASE)
                    and any(re.search(
                        rf"\bCOUNT\s*\(\s*DISTINCT\s+"
                        rf"(?:[A-Za-z_][\w$]*\s*\.\s*)?[`\"\[]?"
                        rf"{re.escape(column_name)}[`\"\]]?\s*\)\s*=\s*"
                        rf"{len(values)}\b",
                        code,
                        re.IGNORECASE,
                    ) for column_name in countable_columns)
                )
                if uses_grouped_all and requirement.get("row_grain"):
                    group = re.search(
                        r"\bGROUP\s+BY\b(?P<body>.*?)(?=\bHAVING\b|"
                        r"\bORDER\s+BY\b|\bLIMIT\b|$)",
                        code,
                        re.IGNORECASE | re.DOTALL,
                    )
                    aliases = self._sql_source_aliases(code)
                    parent_table = str(requirement.get("parent_table") or "")
                    parent_schema = self.schema.tables.get(parent_table)
                    parent_columns = {
                        column.name.casefold()
                        for column in (
                            parent_schema.columns if parent_schema is not None else []
                        )
                    }
                    group_refs = list(re.finditer(
                        r"(?:(?P<qualifier>[A-Za-z_][\w$]*)\s*\.\s*)?"
                        r"[`\"\[]?(?P<column>[A-Za-z_][\w$]*)[`\"\]]?",
                        group.group("body") if group else "",
                        re.IGNORECASE,
                    ))
                    grain_safe = bool(group_refs)
                    for ref in group_refs:
                        qualifier = str(ref.group("qualifier") or "").casefold()
                        column_name = ref.group("column").casefold()
                        physical = aliases.get(qualifier, qualifier) if qualifier else ""
                        if qualifier and physical.casefold() != parent_table.casefold():
                            grain_safe = False
                            break
                        if not qualifier and column_name not in parent_columns:
                            grain_safe = False
                            break
                    uses_grouped_all = uses_grouped_all and grain_safe
                if not (
                    required_tables.issubset(referenced)
                    and literals_present
                    and (uses_intersection or uses_two_exists or uses_grouped_all)
                ):
                    return (
                        "问句要求同一父实体关联的事实列 "
                        f"{requirement.get('value_column')} 同时覆盖 "
                        + "、".join(values)
                        + "；请用两个相关 EXISTS、INTERSECT，或按父实体分组后用 "
                        f"HAVING COUNT(DISTINCT {value_column})={len(values)}。"
                    )

        for requirement in contract.distinct_count_requirements:
            column_name = str(requirement.get("column") or "").rsplit(".", 1)[-1]
            if not re.search(
                rf"\bCOUNT\s*\(\s*DISTINCT\s+(?:[A-Za-z_][\w$]*\s*\.\s*)?"
                rf"[`\"\[]?{re.escape(column_name)}[`\"\]]?\s*\)",
                code,
                re.IGNORECASE,
            ):
                return (
                    f"问句要求按已声明实体键去重计数，候选必须计算 "
                    f"COUNT(DISTINCT {requirement.get('column')})。"
                )

        if contract.relationship_thresholds:
            comparisons = [
                (match.group("operator"), int(match.group("value")))
                for match in re.finditer(
                    r"\bCOUNT\s*\([^)]*\)\s*"
                    r"(?P<operator>>=|<=|<>|!=|=|>|<)\s*(?P<value>\d+)\b",
                    code,
                    re.IGNORECASE | re.DOTALL,
                )
            ]

            def threshold_satisfied(operator: str, value: int) -> bool:
                accepted = {(operator, value)}
                if operator == ">=" and value > 0:
                    accepted.add((">", value - 1))
                elif operator == ">":
                    accepted.add((">=", value + 1))
                elif operator == "<" and value > 0:
                    accepted.add(("<=", value - 1))
                elif operator == "<=":
                    accepted.add(("<", value + 1))
                return any(item in accepted for item in comparisons)

            missing_thresholds = [
                item for item in contract.relationship_thresholds
                if not threshold_satisfied(
                    str(item.get("operator") or ""), int(item.get("value") or 0),
                )
            ]
            if missing_thresholds:
                rendered = "、".join(
                    f"{item.get('subject')} {item.get('operator')} {item.get('value')}"
                    for item in missing_thresholds
                )
                return (
                    "问句声明了关联数量阈值 " + rendered
                    + "，但 SQL 中没有对应的 COUNT 比较。仅出现 HAVING/EXISTS 关键字"
                    "不能证明阈值已实现。"
                )

        if "distinct_entity_count" in contract.required_operators and re.search(
            r"\bCOUNT\s*\(", code, re.IGNORECASE,
        ) and not re.search(
            r"\bCOUNT\s*\(\s*DISTINCT\b|\bSELECT\s+DISTINCT\b",
            code, re.IGNORECASE,
        ):
            return "问句明确要求去重实体数量，但当前 COUNT 没有 DISTINCT/等价去重子查询。请按实体键去重后计数。"

        if contract.comparison_quantifier:
            direction = contract.comparison_direction
            existential = contract.comparison_quantifier == "existential_any"
            expected = "MIN" if (direction == "greater") == existential else "MAX"
            explicit_operator = "ANY" if existential else "ALL"
            expected_comparator = ">" if direction == "greater" else "<"
            explicit = re.search(
                r"(?P<comparator>[<>])\s*\b(?P<quantifier>ANY|ALL)\s*\(",
                code,
                re.IGNORECASE,
            )
            if explicit and (
                explicit.group("quantifier").upper() != explicit_operator
                or explicit.group("comparator") != expected_comparator
            ):
                return (
                    f"问句要求 {direction} {explicit_operator}，候选却使用 "
                    f"{explicit.group('comparator')} {explicit.group('quantifier').upper()}。"
                    f"比较方向必须是 {expected_comparator} {explicit_operator}。"
                )
            if explicit is None:
                aggregate = re.search(
                    r"(?P<comparator>[<>])\s*\(\s*SELECT\s+(?:DISTINCT\s+)?"
                    r"(?P<agg>MIN|MAX)\s*\(",
                    code, re.IGNORECASE | re.DOTALL,
                )
                if aggregate and (
                    aggregate.group("agg").upper() != expected
                    or aggregate.group("comparator") != expected_comparator
                ):
                    return (
                        f"问句的 {contract.comparison_quantifier} 是关系代数量词："
                        f"{direction} 比较应使用 {expected} 边界（或显式 {explicit_operator}），"
                        f"且比较符应为 {expected_comparator}；当前使用 "
                        f"{aggregate.group('comparator')} {aggregate.group('agg').upper()}，"
                        "会改变满足集合。"
                    )
                exists = re.search(
                    r"\bEXISTS\s*\(.*?"
                    r"(?:[A-Za-z_][\w$]*\s*\.\s*)?[A-Za-z_][\w$]*\s*"
                    r"(?P<comparator>[<>])\s*"
                    r"(?:[A-Za-z_][\w$]*\s*\.\s*)?[A-Za-z_][\w$]*",
                    code,
                    re.IGNORECASE | re.DOTALL,
                )
                if exists and exists.group("comparator") != expected_comparator:
                    return (
                        f"问句要求 {direction} 的 {contract.comparison_quantifier}，"
                        f"但相关 EXISTS 内使用 {exists.group('comparator')} 比较；"
                        f"应使用 {expected_comparator}，否则量词方向相反。"
                    )

        if "exact_output_projection" in contract.required_operators \
                and contract.output_layout:
            layout_matches = len(projection_items) == len(contract.output_layout)
            if layout_matches:
                for item, expected in zip(projection_items, contract.output_layout):
                    kind = str(expected.get("kind") or "")
                    if kind == "column":
                        observed = self._simple_projection_columns(
                            f"SELECT {item} FROM __dbagent_projection_scope"
                        )
                        if observed != [str(expected.get("column") or "").casefold()]:
                            layout_matches = False
                            break
                    elif kind == "aggregate":
                        function = str(expected.get("function") or "").upper()
                        aggregate_item = re.sub(
                            r"\s+AS\s+(?:\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|"
                            r"[A-Za-z_][\w$]*)\s*$",
                            "",
                            item.strip(),
                            flags=re.IGNORECASE,
                        )
                        if function != "COUNT" or re.fullmatch(
                            r"COUNT\s*\(\s*(?:DISTINCT\s+)?"
                            r"(?:\*|(?:[A-Za-z_][\w$]*\s*\.\s*)?"
                            r"(?:\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|"
                            r"[A-Za-z_][\w$]*))\s*\)",
                            aggregate_item,
                            re.IGNORECASE,
                        ) is None or re.search(
                            r"\bDISTINCT\b", aggregate_item, re.IGNORECASE,
                        ):
                            layout_matches = False
                            break
                    else:
                        layout_matches = False
                        break
            if not layout_matches:
                rendered = []
                for item in contract.output_layout:
                    if item.get("kind") == "column":
                        rendered.append(str(item.get("column") or ""))
                    else:
                        rendered.append(
                            f"{item.get('function')}({item.get('source_table')}.*)"
                        )
                return (
                    "schema 已将问句的混合输出按顺序绑定为 "
                    + "、".join(rendered)
                    + "；当前 SELECT 遗漏、增加或重排了实体列与聚合列。"
                    "请在实体粒度上同时返回标签和关联记录数。"
                )

        if "exact_output_projection" in contract.required_operators \
                and contract.output_columns and not contract.output_layout:
            observed_projection = self._simple_projection_columns(sql)
            expected_projection = [
                name.casefold() for name in contract.output_columns
            ]
            if observed_projection != expected_projection \
                    or len(projection_items) != len(expected_projection):
                return (
                    "schema 已将问句输出完整绑定为 "
                    + "、".join(contract.output_columns)
                    + "；当前 SELECT 增加、遗漏或重排了输出表达式。"
                    "聚合、筛选、分组和排序辅助列不得作为额外结果返回。"
                )

        for bundle in contract.output_bundles:
            missing = [name for name in bundle if name.casefold() not in projection_columns]
            if missing:
                exact_outputs = "、".join(contract.output_columns or bundle)
                return (
                    "业务字典把一个输出概念明确映射为多个独立列（"
                    + ", ".join(bundle)
                    + "），但当前投影把它们合并成表达式或遗漏了列："
                    + ", ".join(missing)
                    + "。请按字典顺序分别返回这些物理列；SELECT 只保留 "
                    + exact_outputs
                    + "，不要额外返回仅用于筛选、聚合或排序的列。"
                )

        if "separate_projection_atoms" in contract.required_operators and contract.output_columns:
            expected = {name.casefold() for name in contract.output_columns}
            if projection_columns != expected or len(projection_items) != len(contract.output_columns):
                return (
                    "业务字典已经完整限定输出原子列为 "
                    + "、".join(contract.output_columns)
                    + "。当前 SELECT 仍增加了表达式或其他列；请只按该顺序返回这些独立列，"
                    "聚合/排序指标只放在 GROUP BY 或 ORDER BY 所需位置。"
                )

        if "rank_projection" in contract.required_operators and contract.output_columns:
            expected = {name.casefold() for name in contract.output_columns}
            rank_items = [
                item for item in projection_items
                if re.search(
                    r"\b(?:RANK|DENSE_RANK|ROW_NUMBER)\s*\(.*?\)\s*OVER\s*\(",
                    item, re.IGNORECASE | re.DOTALL,
                )
            ]
            if (
                projection_columns != expected
                or len(rank_items) != 1
                or len(projection_items) != len(contract.output_columns) + 1
            ):
                return (
                    "排名查询的输出合同为 "
                    + "、".join(contract.output_columns)
                    + "，再加一个排名列。当前 SELECT 仍有未请求列或缺失合同列；"
                    "请不要额外返回学校名称、筛选字段或其他排序辅助列。"
                )

        for qualified in contract.modifier_filters:
            raw_name = qualified.rsplit(".", 1)[-1]
            if raw_name.casefold() in projection_columns:
                exact_outputs = "、".join(contract.output_columns) or "被限定的业务值列"
                return (
                    f"{qualified} 在问句中限定另一个输出概念，是筛选标志而不是返回列；"
                    f"请把它保留在 WHERE/HAVING 条件中，并从 SELECT 投影移除。SELECT 只返回 "
                    f"{exact_outputs}。"
                )
            predicate_sections = re.findall(
                r"\b(?:WHERE|HAVING)\b(.*?)(?=\bGROUP\s+BY\b|\bORDER\s+BY\b|"
                r"\bLIMIT\b|\bUNION\b|\bINTERSECT\b|\bEXCEPT\b|$)",
                code,
                re.IGNORECASE | re.DOTALL,
            )
            if not any(
                re.search(
                    rf"(?<![A-Za-z0-9_$]){re.escape(raw_name)}"
                    rf"(?![A-Za-z0-9_$])",
                    section,
                    re.IGNORECASE,
                )
                for section in predicate_sections
            ):
                return (
                    f"{qualified} 是问句明确的修饰词筛选，但候选 SQL 的 WHERE/HAVING "
                    "没有使用该标志。只从 SELECT 移除标志列仍不足以实现筛选语义。"
                )
        if contract.modifier_filters and contract.output_columns:
            expected = {name.casefold() for name in contract.output_columns}
            if projection_columns != expected or len(projection_items) != len(expected):
                return (
                    "修饰词筛选的输出合同只允许 "
                    + "、".join(contract.output_columns)
                    + "。当前仍有未请求输出或表达式；标志列只用于 WHERE/HAVING。"
                )

        if contract.tie_policy == "all_ties" and re.search(r"\bLIMIT\s+1\b", code, re.IGNORECASE):
            return "问句的复数或显式并列表达要求保留并列结果，但当前 LIMIT 1 会丢弃并列项。请使用并列排名/极值子查询返回全部并列结果。"
        if contract.tie_policy == "single_row" and not re.search(
            r"\b(?:LIMIT\s+1|TOP\s+1|FETCH\s+(?:FIRST|NEXT)\s+1\s+ROWS?\s+ONLY)\b",
            code,
            re.IGNORECASE,
        ):
            return (
                "问句以单数实体要求最高/最低/最多/最少的一条结果，但当前查询可能返回"
                "全部并列项。请用明确排序和单行限制返回一条；除非用户明确要求并列，"
                "不要用等于全局极值的 HAVING/WHERE 保留多行。"
            )
        if contract.tie_policy == "single_row" and contract.tie_breaker_columns:
            order = re.search(
                r"\bORDER\s+BY\b(?P<body>.*?)(?=\bLIMIT\b|\bFETCH\b|$)",
                code,
                re.IGNORECASE | re.DOTALL,
            )
            aliases = self._sql_source_aliases(code)

            def ordered_tie_breaker(qualified: str) -> bool:
                if order is None or "." not in qualified:
                    return False
                table_name, column_name = qualified.rsplit(".", 1)
                equivalent_refs = [(table_name, column_name)]
                for table in self.schema.tables.values():
                    for column in table.columns:
                        if str(column.fk_table or "").casefold() \
                                == table_name.casefold() \
                                and str(column.fk_column or "").casefold() \
                                == column_name.casefold():
                            equivalent_refs.append((table.name, column.name))
                for physical_table, physical_column in equivalent_refs:
                    qualifiers = [
                        alias for alias, physical in aliases.items()
                        if physical.casefold() == physical_table.casefold()
                    ]
                    if qualifiers and re.search(
                        rf"(?<![A-Za-z0-9_$])"
                        rf"[`\"\[]?(?:{'|'.join(map(re.escape, qualifiers))})"
                        rf"[`\"\]]?"
                        rf"\s*\.\s*[`\"\[]?{re.escape(physical_column)}[`\"\]]?"
                        rf"(?![A-Za-z0-9_$])",
                        order.group("body"),
                        re.IGNORECASE,
                    ):
                        return True
                    owners = [
                        table.name for table in self.schema.tables.values()
                        if table.name in referenced_tables and any(
                            column.name.casefold() == physical_column.casefold()
                            for column in table.columns
                        )
                    ]
                    if len(owners) == 1 \
                            and owners[0].casefold() == physical_table.casefold() \
                            and re.search(
                                rf"(?<![A-Za-z0-9_$.])[`\"\[]?"
                                rf"{re.escape(physical_column)}[`\"\]]?"
                                rf"(?![A-Za-z0-9_$])",
                                order.group("body"),
                                re.IGNORECASE,
                            ) is not None:
                        return True
                return False

            missing_tie_breakers = [
                qualified
                for qualified in contract.tie_breaker_columns
                if not ordered_tie_breaker(qualified)
            ]
            if missing_tie_breakers:
                return (
                    "问句要求单行极值结果，但主排序值可能并列；"
                    "候选 SQL 没有使用已证明的实体键作为稳定二级排序："
                    + "、".join(missing_tie_breakers)
                    + "。请在主排序后按这些键排序，再 LIMIT 1，保证重试可复现。"
                )
        return ""

    @staticmethod
    def _split_projection(projection: str) -> List[str]:
        """按顶层逗号拆 SELECT 投影；复杂表达式保守留给模型与数据库。"""
        items: List[str] = []
        start = 0
        depth = 0
        quote = ""
        index = 0
        while index < len(projection):
            char = projection[index]
            if quote:
                if char == quote:
                    if index + 1 < len(projection) and projection[index + 1] == quote:
                        index += 1
                    else:
                        quote = ""
            elif char in "'\"`":
                quote = char
            elif char == "[":
                quote = "]"
            elif char == "(":
                depth += 1
            elif char == ")" and depth:
                depth -= 1
            elif char == "," and depth == 0:
                items.append(projection[start:index].strip())
                start = index + 1
            index += 1
        items.append(projection[start:].strip())
        return [item for item in items if item]

    def _projection_conflict(
        self, question: str, sql: str,
    ) -> Optional[QuerySemanticConflict]:
        """识别投影偏差并保留可机读的必需/禁止列。"""
        if re.search(
            r"\b(?:all\s+(?:columns|fields)|full\s+details?)\b|全部字段|所有字段|完整信息",
            question,
            re.IGNORECASE,
        ):
            return ""
        projection = self._top_level_projection(sql)
        if projection is None:
            return ""
        projection_items = self._split_projection(projection)
        simple_column = re.compile(
            r"^(?:DISTINCT\s+)?(?:(?P<table>[A-Za-z_][\w$]*)\s*\.\s*)?"
            r"(?:\"([^\"]+)\"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))"
            r"(?:\s+AS\s+(?:\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*))?$",
            re.IGNORECASE,
        )
        alias_pattern = re.compile(
            r"\s+AS\s+(?:\"(?P<double>[^\"]+)\"|`(?P<backtick>[^`]+)`|"
            r"\[(?P<bracket>[^\]]+)\]|(?P<plain>[A-Za-z_][\w$]*))\s*$",
            re.IGNORECASE,
        )
        tables_by_folded = {name.casefold(): name for name in self.schema.tables}
        known_by_table = {
            table_name: {column.name.casefold(): column.name for column in table.columns}
            for table_name, table in self.schema.tables.items()
        }
        objects_by_table = {
            table_name: {column.name.casefold(): column for column in table.columns}
            for table_name, table in self.schema.tables.items()
        }
        table_tokens = {
            table_name: self._normalized_language_tokens(table_name)
            for table_name in self.schema.tables
        }
        known = {
            folded: canonical
            for table_columns in known_by_table.values()
            for folded, canonical in table_columns.items()
        }
        aliases: Dict[str, str] = {}
        for source_match in re.finditer(
            r"\b(?:FROM|JOIN)\s+([A-Za-z_][\w$]*)"
            r"(?:\s+(?:AS\s+)?(?!ON\b|WHERE\b|JOIN\b|GROUP\b|ORDER\b|LIMIT\b)"
            r"([A-Za-z_][\w$]*))?",
            sql,
            re.IGNORECASE,
        ):
            physical = tables_by_folded.get(source_match.group(1).casefold())
            if physical:
                aliases[source_match.group(1).casefold()] = physical
                if source_match.group(2):
                    aliases[source_match.group(2).casefold()] = physical
        referenced_tables = self._sql_referenced_tables(sql)
        sole_table = referenced_tables[0] if len(referenced_tables) == 1 else None
        selected: List[tuple[Optional[str], str]] = []
        selected_alias_tokens: Dict[tuple[Optional[str], str], set[str]] = {}
        expression_alias_tokens: set[str] = set()
        for item in projection_items:
            column_match = simple_column.fullmatch(item.strip())
            if column_match is None:
                alias_match = alias_pattern.search(item)
                if alias_match:
                    alias_name = next(
                        value for value in alias_match.groupdict().values()
                        if value is not None
                    )
                    expression_alias_tokens.update(
                        self._normalized_language_tokens(alias_name)
                    )
                continue
            qualifier = column_match.group("table")
            raw = next(
                value for value in column_match.groups()[1:] if value is not None
            )
            table_name = aliases.get(qualifier.casefold()) if qualifier else sole_table
            canonical = (
                known_by_table.get(table_name, {}).get(raw.casefold())
                if table_name else known.get(raw.casefold())
            )
            item_key = (table_name, canonical) if canonical else None
            if item_key and item_key not in selected:
                selected.append(item_key)
            alias_match = alias_pattern.search(item)
            if item_key and alias_match:
                alias_name = next(
                    value for value in alias_match.groupdict().values()
                    if value is not None
                )
                selected_alias_tokens[item_key] = self._normalized_language_tokens(
                    alias_name
                )
        if not selected:
            return ""
        output_phrase = self._output_request_phrase(question)
        output_tokens = self._normalized_language_tokens(output_phrase)
        question_text = str(question or "").partition(
            "Relevant business evidence supplied by the user:"
        )[0]
        connector_tokens = {"of", "the"}

        # Generic identifiers need their entity noun to resolve the physical
        # key. ``id and weight of all pets`` cannot be decided from the token
        # ``id`` alone because the schema column is commonly ``PetID``. Keep
        # this deliberately bounded to an explicit ID/identifier output before
        # an ``of <entity>`` phrase; foreign keys on other joined tables must
        # not be promoted merely because the entity appears elsewhere.
        identifier_entity_tokens: set[str] = set()
        for entity_match in re.finditer(
            r"\b(?:ids?|identifiers?)\b"
            r"(?:(?!\b(?:where|whose|that|with|having|who|by|on)\b)[^?.]){0,96}?"
            r"\bof\s+(?:(?:the|all)\s+)?(?P<entity>[A-Za-z][\w-]*)",
            question_text,
            re.IGNORECASE,
        ):
            identifier_entity_tokens.update(
                self._normalized_language_tokens(entity_match.group("entity"))
            )

        def physical_output_tokens(column_name: str) -> set[str]:
            tokens = self._normalized_language_tokens(column_name) - connector_tokens
            # ``treatment_type_description`` is conventionally requested as
            # "treatment description"; ``type`` is a schema qualifier rather
            # than an additional requested output atom in this shape.
            if "description" in tokens and len(tokens) >= 3:
                tokens.discard("type")
            return tokens

        def entity_identifier_match(
            table_name: Optional[str], column_name: str,
        ) -> bool:
            if not table_name or not identifier_entity_tokens:
                return False
            column_tokens = physical_output_tokens(column_name)
            if not column_tokens & {"id", "identifier"}:
                return False
            entity_tokens = table_tokens.get(table_name, set())
            if not entity_tokens & identifier_entity_tokens:
                return False
            column = objects_by_table.get(table_name, {}).get(column_name.casefold())
            return bool(column and (
                column.pk or column_tokens & entity_tokens
                or column_tokens <= ({"id", "identifier"} | entity_tokens)
            ))
        generic_concept_tokens = {
            "a", "all", "an", "anti", "average", "column", "count", "data",
            "date", "field", "first", "id", "identifier", "last", "length",
            "maximum", "minimum", "name", "number", "result", "status", "the",
            "time", "total", "type", "value", "what", "which", "who",
        }
        token_document_frequency: Dict[str, int] = {}
        total_schema_columns = 0
        for table_name, table in self.schema.tables.items():
            for column in table.columns:
                total_schema_columns += 1
                concept_tokens = (
                    self._normalized_language_tokens(column.name)
                    | self._normalized_language_tokens(" ".join((
                        column.semantic_name, column.description,
                    )))
                    | table_tokens.get(table_name, set())
                )
                for token in concept_tokens:
                    token_document_frequency[token] = (
                        token_document_frequency.get(token, 0) + 1
                    )
        rare_limit = max(3, int(math.ceil(total_schema_columns * 0.08)))
        discriminative_output_tokens = {
            token for token in output_tokens
            if len(token) >= 4
            and token not in generic_concept_tokens
            and token in token_document_frequency
            and token_document_frequency.get(token, total_schema_columns + 1) <= rare_limit
        }
        if discriminative_output_tokens:
            rarest_frequency = min(
                token_document_frequency[token]
                for token in discriminative_output_tokens
            )
            discriminative_output_tokens = {
                token for token in discriminative_output_tokens
                if token_document_frequency[token] == rarest_frequency
            }
        mentioned: List[str] = []
        unmentioned: List[str] = []
        selected_concepts: List[tuple[Optional[str], set[str], bool, bool]] = []
        for _table_name, column_name in selected:
            column_tokens = physical_output_tokens(column_name)
            comparable_column_tokens = column_tokens - connector_tokens
            column = objects_by_table.get(_table_name, {}).get(column_name.casefold()) \
                if _table_name else None
            metadata_tokens = self._normalized_language_tokens(" ".join((
                column.semantic_name, column.description,
            ))) if column is not None else set()
            metadata_overlap = metadata_tokens & output_tokens
            entity_tokens = table_tokens.get(_table_name or "", set())
            concept = column_tokens | metadata_tokens | entity_tokens
            opaque_column = not any(len(token) >= 4 for token in column_tokens)
            entity_name_match = self._entity_name_match(
                column_tokens, metadata_tokens, output_tokens,
            )
            preserves_specific_concept = bool(
                not discriminative_output_tokens
                or concept & discriminative_output_tokens
            )
            output_alias_match = bool(
                selected_alias_tokens.get((_table_name, column_name))
                and selected_alias_tokens[(_table_name, column_name)].issubset(output_tokens)
            )
            selected_concepts.append((
                _table_name, concept, bool(metadata_tokens), opaque_column,
            ))
            if (
                comparable_column_tokens
                and comparable_column_tokens.issubset(output_tokens)
            ) or (
                opaque_column
                and len(metadata_overlap) >= 3
                and len(metadata_overlap) / max(1, len(metadata_tokens)) >= 0.45
                and preserves_specific_concept
            ) or entity_name_match or entity_identifier_match(
                _table_name, column_name,
            ) or output_alias_match:
                mentioned.append(column_name)
            else:
                unmentioned.append(column_name)
        warnings: List[str] = []
        if len(selected) >= 2 and mentioned and unmentioned:
            warnings.append(
                "疑似增加了用户未请求的简单列：" + ", ".join(unmentioned)
            )

        selected_keys = {(table_name, column_name.casefold()) for table_name, column_name in selected}
        missing: List[str] = []
        for table_name in {table for table, _column in selected if table}:
            for folded, canonical in known_by_table.get(table_name, {}).items():
                if (table_name, folded) in selected_keys:
                    continue
                tokens = physical_output_tokens(canonical)
                comparable_tokens = tokens - connector_tokens
                candidate = objects_by_table.get(table_name, {}).get(folded)
                metadata_tokens = self._normalized_language_tokens(" ".join((
                    candidate.semantic_name, candidate.description,
                ))) if candidate is not None else set()
                concept = tokens | metadata_tokens | table_tokens.get(table_name, set())
                opaque_candidate = not any(len(token) >= 4 for token in tokens)
                entity_name_match = self._entity_name_match(
                    tokens, metadata_tokens, output_tokens,
                )
                numeric_base = re.sub(r"\d+$", "", canonical.casefold())
                numbered_sibling = any(
                    selected_table == table_name
                    and re.sub(r"\d+$", "", selected_name.casefold()) == numeric_base
                    and selected_name.casefold() != canonical.casefold()
                    and bool(concept & output_tokens)
                    for selected_table, selected_name in selected
                )
                shadowed_by_selected = any(
                    selected_table == table_name
                    and all(len(token) >= 2 for token in tokens)
                    and tokens < self._normalized_language_tokens(selected_name)
                    for selected_table, selected_name in selected
                )
                represented_by_expression = bool(
                    expression_alias_tokens
                    and (
                        tokens & expression_alias_tokens
                        or metadata_tokens & expression_alias_tokens
                    )
                )
                sibling_match = any(
                    not shadowed_by_selected
                    and bool(metadata_tokens)
                    and selected_has_metadata
                    and opaque_candidate
                    and selected_is_opaque
                    and concept and selected_concept
                    and len(concept & selected_concept) / max(
                        1, min(len(concept), len(selected_concept)),
                    ) >= 0.55
                    and len(concept & output_tokens) >= 3
                    and (
                        not discriminative_output_tokens
                        or bool(concept & discriminative_output_tokens)
                    )
                    for (
                        selected_table, selected_concept, selected_has_metadata,
                        selected_is_opaque,
                    ) in selected_concepts
                    if selected_table == table_name
                )
                if (
                    not represented_by_expression
                    and (
                        (
                            comparable_tokens
                            and comparable_tokens.issubset(output_tokens)
                            and not shadowed_by_selected
                        )
                        or sibling_match
                        or numbered_sibling
                        or entity_name_match
                        or entity_identifier_match(table_name, canonical)
                    )
                ):
                    missing.append(canonical)
        if missing:
            warnings.append("疑似遗漏问句明确请求的输出列：" + ", ".join(missing))
        if not warnings:
            return ""
        message = (
            "SELECT 投影" + "；".join(warnings)
            + "。请逐项对照问句，只返回全部明确请求的输出列，并保持问句顺序。"
        )
        required_outputs: List[str] = []
        for column_name in [*mentioned, *missing]:
            if column_name not in required_outputs:
                required_outputs.append(column_name)
        if len(required_outputs) >= 2:
            # Candidate SELECT order is not evidence. Reconstruct the order
            # from the user's output phrase so a repair cannot preserve the
            # projection-order error it is meant to correct.
            folded_question = question_text.casefold()

            def requested_position(column_name: str) -> int:
                positions = [
                    match.start()
                    for token in physical_output_tokens(column_name)
                    for match in [re.search(
                        rf"(?<![A-Za-z0-9_]){re.escape(token)}(?![A-Za-z0-9_])",
                        folded_question,
                    )]
                    if match is not None
                ]
                return min(positions) if positions else len(folded_question) + 1

            required_outputs = [
                item[1] for item in sorted(
                    enumerate(required_outputs),
                    key=lambda item: (requested_position(item[1]), item[0]),
                )
            ]
        if len(required_outputs) >= 3:
            family_prefix = os.path.commonprefix([
                column_name.casefold() for column_name in required_outputs
            ]).rstrip(" _-0123456789")
            if len(family_prefix) >= 3:
                required_outputs = sorted(required_outputs, key=str.casefold)
        required_output_bindings: List[dict] = []
        for column_name in required_outputs:
            selected_tables = [
                table_name for table_name, selected_column in selected
                if table_name and selected_column == column_name
            ]
            possible_tables = selected_tables or [
                table_name for table_name, columns in known_by_table.items()
                if column_name.casefold() in columns
            ]
            if len(possible_tables) == 1:
                binding = {"table": possible_tables[0], "column": column_name}
                if binding not in required_output_bindings:
                    required_output_bindings.append(binding)
        return QuerySemanticConflict(
            code="projection",
            message=message,
            constraints={
                "required_output_columns": required_outputs,
                "required_output_bindings": required_output_bindings,
                "forbidden_output_columns": list(dict.fromkeys(unmentioned)),
                "missing_output_columns": list(dict.fromkeys(missing)),
                "output_order": "question_order",
                "minimal_projection": True,
            },
        )

    def _projection_retry_hint(self, question: str, sql: str) -> str:
        conflict = self._projection_conflict(question, sql)
        return conflict.message if conflict else ""

    @staticmethod
    def _set_semantics_retry_hint(question: str, sql: str) -> str:
        """识别 AND 连接的两个明确值被裸 IN 降级为“任一值”。"""
        if re.search(r"\bINTERSECT\b|\bHAVING\b", sql, re.IGNORECASE):
            return ""
        if len(re.findall(r"\bEXISTS\b", sql, re.IGNORECASE)) >= 2:
            return ""
        in_pattern = re.compile(
            r"(?:[A-Za-z_][\w$]*\s*\.\s*)?[A-Za-z_][\w$]*\s+IN\s*\(([^()]*)\)",
            re.IGNORECASE | re.DOTALL,
        )
        question_folded = question.casefold()
        for match in in_pattern.finditer(sql):
            values = [
                (single or double).replace("''", "'").replace('""', '"').strip()
                for single, double in re.findall(
                    r"'((?:''|[^'])*)'|\"((?:\"\"|[^\"])*)\"",
                    match.group(1),
                )
                if (single or double).strip()
            ]
            if len(values) < 2:
                continue
            positions = []
            for value in values:
                start = question_folded.find(value.casefold())
                if start < 0:
                    positions = []
                    break
                positions.append((start, start + len(value), value))
            if len(positions) < 2:
                continue
            positions.sort()
            connectors = [
                question_folded[left[1]:right[0]]
                for left, right in zip(positions, positions[1:])
            ]
            has_and = any(re.search(r"\band\b|和|与|及", text) for text in connectors)
            has_or = any(re.search(r"\bor\b|或", text) for text in connectors)
            if has_and and not has_or:
                return (
                    "问句用 AND/和/与要求同一实体同时满足多个明确值，但当前裸 IN 只保证任一值。"
                    "请使用 INTERSECT、两个 EXISTS，或按父实体 GROUP BY 后用 "
                    "HAVING COUNT(DISTINCT ...)=值数量。"
                )
        return ""

    @staticmethod
    def _parallel_measures_retry_hint(question: str, sql: str) -> str:
        """Distinguish parallel requested counts from one merged IN filter."""
        asks_parallel = bool(re.search(
            r"\bhow\s+many\b.+\bwith\b.+\band\s+with\b|"
            r"\bnumber\s+of\b.+\band\b.+\bnumber\s+of\b|"
            r"分别(?:有多少|统计|计算)|各自(?:有多少|统计|计算)",
            question,
            re.IGNORECASE | re.DOTALL,
        ))
        if not asks_parallel:
            return ""
        projection = re.match(r"\s*SELECT\s+(.*?)\s+FROM\b", sql, re.IGNORECASE | re.DOTALL)
        if projection is None:
            return ""
        items = NL2SQLExecutor._split_projection(projection.group(1))
        in_values = re.search(r"\bIN\s*\(([^()]*)\)", sql, re.IGNORECASE | re.DOTALL)
        if (
            len(items) == 1
            and len(re.findall(r"\bCOUNT\s*\(", projection.group(1), re.IGNORECASE)) == 1
            and in_values is not None
            and len(re.findall(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"", in_values.group(1))) >= 2
        ):
            return (
                "问句要求分别返回两个并列数量，但当前只用一个 COUNT + IN 合并成了一个总数。"
                "请按问句顺序返回两个独立的条件计数列；不要把它改成同一实体的交集筛选。"
            )
        return ""

    @staticmethod
    def _row_grain_retry_hint(question: str, sql: str) -> str:
        """Protect detail-row requests from being collapsed into string aggregates."""
        asks_each_detail = bool(re.search(
            r"\b(?:each|every|all)\b.+\b(?:performed|made|received|has|have|with)\b|"
            r"每个.+(?:明细|记录|执行|发生)",
            question,
            re.IGNORECASE | re.DOTALL,
        )) and not re.search(
            r"\bfirst\b(?!\s+name\b)|\b(?:latest|earliest|one|single|top\s+1|most|least|fewest)\b|"
            r"第一|最新|最早|一条|最多|最少",
            question,
            re.IGNORECASE,
        )
        if asks_each_detail and (
            re.search(r"\bLIMIT\s+1\b", sql, re.IGNORECASE)
            or (
                re.search(r"\bROW_NUMBER\s*\(", sql, re.IGNORECASE)
                and re.search(
                    r"\b(?:rn|row_num|row_number)\s*=\s*1\b",
                    sql,
                    re.IGNORECASE,
                )
            )
        ):
            return (
                "问句要求每个实体的所有关联明细，当前 LIMIT 1 或 "
                "ROW_NUMBER()=1 把多条明细压成了一条。请保持一行一条关联明细。"
            )
        lists_entities_by_dimension = bool(re.search(
            r"\b(?:list|show)\b.+\b(?:all|each|every)\b.+\bby\b|"
            r"列出.+(?:全部|每个).+按.+(?:分组|分类)",
            question,
            re.IGNORECASE | re.DOTALL,
        ))
        if lists_entities_by_dimension and re.search(
            r"\b(?:GROUP_CONCAT|STRING_AGG|JSON_GROUP_ARRAY|ARRAY_AGG)\s*\(",
            sql,
            re.IGNORECASE,
        ):
            return (
                "问句要求列出实体明细并按维度组织，当前字符串/数组聚合把多条实体压成了少量分组行。"
                "请保持一行一个实体，并把维度与实体标识作为普通输出/分组列；不要拼接实体列表。"
            )
        return ""

    @staticmethod
    def _numeric_result_type_retry_hint(question: str, sql: str) -> str:
        """Keep requested rounded percentages numeric instead of formatted text."""
        asks_decimal_places = bool(re.search(
            r"\b(?:to|with)\s+\w+\s+decimal\s+places?\b|"
            r"\b\d+\s+decimal\s+places?\b|保留\s*[一二三四五六七八九十\d]+\s*位",
            question,
            re.IGNORECASE,
        ))
        if asks_decimal_places and re.search(r"\b(?:PRINTF|FORMAT)\s*\(", sql, re.IGNORECASE):
            return (
                "问句要求按指定位数给出数值，但 PRINTF/FORMAT 会把结果变成文本。"
                "请使用数据库的数值舍入函数（SQLite 使用 ROUND(表达式, 位数)），"
                "保持结果列为数值类型。"
            )
        return ""

    def _date_storage_retry_hint(self, sql: str) -> str:
        """用 schema 抽样值识别 SQLite 日期位置/存储格式的高置信偏差。"""
        tables_by_folded = {name.casefold(): name for name in self.schema.tables}
        columns_by_table = {
            table_name: {column.name.casefold(): column for column in table.columns}
            for table_name, table in self.schema.tables.items()
        }
        aliases: Dict[str, str] = {}
        for source_match in re.finditer(
            r"\b(?:FROM|JOIN)\s+"
            r"(?:\"([^\"]+)\"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))"
            r"(?:\s+(?:AS\s+)?(?!ON\b|WHERE\b|JOIN\b|GROUP\b|ORDER\b|LIMIT\b)"
            r"([A-Za-z_][\w$]*))?",
            sql,
            re.IGNORECASE,
        ):
            raw_table = next(
                value for value in source_match.groups()[:4] if value is not None
            )
            physical = tables_by_folded.get(raw_table.casefold())
            if physical:
                aliases[raw_table.casefold()] = physical
                if source_match.group(5):
                    aliases[source_match.group(5).casefold()] = physical

        referenced_tables = self._sql_referenced_tables(sql)

        def _column(qualifier: Optional[str], raw_name: str) -> Optional[DBColumn]:
            name = raw_name.strip('"`[]').casefold()
            if qualifier:
                table_name = aliases.get(qualifier.casefold())
                return columns_by_table.get(table_name, {}).get(name) if table_name else None
            matches = [
                columns_by_table[table_name][name]
                for table_name in referenced_tables
                if name in columns_by_table.get(table_name, {})
            ]
            return matches[0] if len(matches) == 1 else None

        def _has_iso_date_sample(column: Optional[DBColumn]) -> bool:
            if column is None or not is_time_column(column):
                return False
            return any(
                re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:[ T].*)?", str(value).strip())
                for value in column.sample_values
                if value is not None
            )

        def _has_iso_datetime_sample(column: Optional[DBColumn]) -> bool:
            if column is None or not is_time_column(column):
                return False
            return any(
                re.fullmatch(r"\d{4}-\d{2}-\d{2}[ T].+", str(value).strip())
                for value in column.sample_values
                if value is not None
            )

        reference = (
            r"(?:(?P<qualifier>[A-Za-z_][\w$]*)\s*\.\s*)?"
            r"(?P<column>\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*)"
        )
        substring = re.compile(
            r"\bSUBSTR(?:ING)?\s*\(\s*" + reference + r"\s*,\s*5\s*,\s*2\s*\)",
            re.IGNORECASE,
        )
        for match in substring.finditer(sql):
            if _has_iso_date_sample(_column(match.group("qualifier"), match.group("column"))):
                return (
                    "日期列抽样值采用 YYYY-MM-DD，而 SQLite SUBSTR 从 1 开始计数；"
                    "月份位于第 6-7 个字符，应使用 SUBSTR(日期列, 6, 2)，不能从第 5 位开始。"
                )

        date_literal = re.compile(
            reference + r"\s*=\s*'(?P<year>\d{4})/(?P<month>\d{1,2})/(?P<day>\d{1,2})'",
            re.IGNORECASE,
        )
        for match in date_literal.finditer(sql):
            if _has_iso_date_sample(_column(match.group("qualifier"), match.group("column"))):
                normalized = (
                    f"{match.group('year')}-{int(match.group('month')):02d}-"
                    f"{int(match.group('day')):02d}"
                )
                return (
                    "日期列抽样值采用 YYYY-MM-DD，但当前等值条件使用斜杠日期。"
                    f"请按真实存储格式改为 '{normalized}'；不要依赖 SQLite 隐式转换。"
                )

        iso_date_equality = re.compile(
            reference + r"\s*=\s*'(?P<date>\d{4}-\d{2}-\d{2})'",
            re.IGNORECASE,
        )
        for match in iso_date_equality.finditer(sql):
            if _has_iso_datetime_sample(
                _column(match.group("qualifier"), match.group("column"))
            ):
                literal = match.group("date")
                return (
                    "日期列抽样值包含时间部分（YYYY-MM-DDTHH:MM:SS 或"
                    " YYYY-MM-DD HH:MM:SS），但当前用纯日期做字符串等值比较，"
                    "会漏掉当天记录。请按真实存储格式改用日期前缀匹配"
                    f"（例如 LIKE '{literal}%'）或显式取日期后比较。"
                )
        return ""

    def _enum_literal_case_retry_hint(self, sql: str) -> str:
        """依据真实值域抽样识别大小写不一致的枚举等值条件。"""
        tables_by_folded = {name.casefold(): name for name in self.schema.tables}
        columns_by_table = {
            table_name: {column.name.casefold(): column for column in table.columns}
            for table_name, table in self.schema.tables.items()
        }
        aliases: Dict[str, str] = {}
        for source_match in re.finditer(
            r"\b(?:FROM|JOIN)\s+"
            r"(?:\"([^\"]+)\"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))"
            r"(?:\s+(?:AS\s+)?(?!ON\b|WHERE\b|JOIN\b|GROUP\b|ORDER\b|LIMIT\b)"
            r"([A-Za-z_][\w$]*))?",
            sql,
            re.IGNORECASE,
        ):
            raw_table = next(
                value for value in source_match.groups()[:4] if value is not None
            )
            physical = tables_by_folded.get(raw_table.casefold())
            if physical:
                aliases[raw_table.casefold()] = physical
                if source_match.group(5):
                    aliases[source_match.group(5).casefold()] = physical

        referenced_tables = self._sql_referenced_tables(sql)

        def _column(qualifier: Optional[str], raw_name: str) -> Optional[DBColumn]:
            name = raw_name.strip('"`[]').casefold()
            if qualifier:
                table_name = aliases.get(qualifier.casefold())
                return columns_by_table.get(table_name, {}).get(name) if table_name else None
            matches = [
                columns_by_table[table_name][name]
                for table_name in referenced_tables
                if name in columns_by_table.get(table_name, {})
            ]
            return matches[0] if len(matches) == 1 else None

        reference = (
            r"(?:(?P<qualifier>[A-Za-z_][\w$]*)\s*\.\s*)?"
            r"(?P<column>\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*)"
        )
        equality = re.compile(
            reference + r"\s*=\s*'(?P<literal>(?:''|[^'])*)'",
            re.IGNORECASE,
        )
        mismatches: List[tuple[str, str, str]] = []
        for match in equality.finditer(sql):
            literal = match.group("literal").replace("''", "'")
            if not literal:
                continue
            column = _column(match.group("qualifier"), match.group("column"))
            if column is None:
                continue
            canonical = {
                str(value).strip()
                for value in column.sample_values
                if value is not None
                and str(value).strip() != literal
                and str(value).strip().casefold() == literal.casefold()
            }
            if len(canonical) == 1:
                mismatches.append((column.name, literal, next(iter(canonical))))
        if not mismatches:
            return ""
        details = "；".join(
            f"{column} 实际抽样值为 {actual!r}，当前字面量为 {literal!r}"
            for column, literal, actual in mismatches
        )
        return (
            "当前等值条件与 schema 真实值域仅大小写不同："
            + details
            + "。请使用抽样值的精确大小写；不要假设数据库使用不区分大小写的排序规则。"
        )

    def _wildcard_literal_retry_hint(self, question: str, sql: str) -> str:
        """Reject unrequested wildcard broadening of an exact sampled value."""
        if self._question_requests_fuzzy_matching(question):
            return ""
        tables_by_folded = {name.casefold(): name for name in self.schema.tables}
        columns_by_table = {
            table_name: {column.name.casefold(): column for column in table.columns}
            for table_name, table in self.schema.tables.items()
        }
        aliases: Dict[str, str] = {}
        for source_match in re.finditer(
            r"\b(?:FROM|JOIN)\s+"
            r"(?:\"([^\"]+)\"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))"
            r"(?:\s+(?:AS\s+)?(?!ON\b|WHERE\b|JOIN\b|GROUP\b|ORDER\b|LIMIT\b)"
            r"([A-Za-z_][\w$]*))?",
            sql,
            re.IGNORECASE,
        ):
            raw_table = next(
                value for value in source_match.groups()[:4] if value is not None
            )
            physical = tables_by_folded.get(raw_table.casefold())
            if physical:
                aliases[raw_table.casefold()] = physical
                if source_match.group(5):
                    aliases[source_match.group(5).casefold()] = physical
        referenced_tables = self._sql_referenced_tables(sql)

        def column_for(qualifier: Optional[str], raw_name: str) -> Optional[DBColumn]:
            folded = raw_name.strip('"`[]').casefold()
            if qualifier:
                table_name = aliases.get(qualifier.casefold())
                return columns_by_table.get(table_name, {}).get(folded) \
                    if table_name else None
            matches = [
                columns_by_table[table_name][folded]
                for table_name in referenced_tables
                if folded in columns_by_table.get(table_name, {})
            ]
            return matches[0] if len(matches) == 1 else None

        like = re.compile(
            r"(?:(?P<qualifier>[A-Za-z_][\w$]*)\s*\.\s*)?"
            r"(?P<column>\"[^\"]+\"|`[^`]+`|\[[^\]]+\]|[A-Za-z_][\w$]*)"
            r"\s+LIKE\s+(?P<quote>['\"])(?P<literal>.*?)(?P=quote)",
            re.IGNORECASE | re.DOTALL,
        )
        question_tokens = self._normalized_language_tokens(question)
        broadened: List[tuple[str, str, str]] = []
        for match in like.finditer(sql):
            literal = match.group("literal")
            if not re.search(r"[%_]", literal):
                continue
            core = re.sub(r"[%_]", " ", literal)
            core = re.sub(r"\s+", " ", core).strip()
            if not core:
                continue
            core_tokens = self._normalized_language_tokens(core)
            if not core_tokens or not core_tokens.issubset(question_tokens):
                continue
            column = column_for(match.group("qualifier"), match.group("column"))
            if column is None:
                continue
            canonical = {
                str(value).strip()
                for value in column.sample_values
                if value is not None
                and str(value).strip().casefold() == core.casefold()
            }
            if len(canonical) == 1:
                broadened.append((column.name, literal, next(iter(canonical))))
        if not broadened:
            return ""
        details = "；".join(
            f"{column} LIKE {literal!r}，而精确抽样值为 {actual!r}"
            for column, literal, actual in broadened
        )
        return (
            "问句给出了可精确落到 schema 真实值域的类别值，但候选 SQL "
            "未经用户要求就扩大成通配匹配：" + details
            + "。请使用精确等值；只有用户明确要求包含、前缀、后缀或模糊匹配时才使用通配符。"
        )

    def _semantic_conflict(
        self,
        question: str,
        sql: str,
        *,
        locked_projection_columns: Optional[List[str]] = None,
    ) -> Optional[QuerySemanticConflict]:
        """Return typed, high-confidence local evidence; never rewrite SQL."""
        relational_hint = self._relational_algebra_retry_hint(
            question, sql, self.last_relational_contract,
        )
        if relational_hint:
            return QuerySemanticConflict(
                code="relational_algebra_contract",
                message=relational_hint,
                constraints={
                    "relational_contract": self.last_relational_contract.as_dict(),
                },
            )
        if locked_projection_columns is not None:
            expected = [str(item).casefold() for item in locked_projection_columns]
            observed = self._simple_projection_columns(sql)
            if observed != expected:
                return QuerySemanticConflict(
                    code="locked_projection_mismatch",
                    message=(
                        "本地编译后的 SELECT 投影与锁定输出合同不一致；"
                        "不允许增加、删除、合并或重排输出列。"
                    ),
                    constraints={
                        "expected_output_columns": list(locked_projection_columns),
                        "observed_output_columns": observed,
                    },
                )
        authoritative_projection = bool(
            self.last_relational_contract.output_columns
            and self.last_relational_contract.output_bindings
            and set(self.last_relational_contract.evidence) & {
                "explicit_dictionary_tuple",
                "adjective_maps_to_schema_flag",
                "schema_bound_output_phrase",
            }
        )
        checks = (
            ("join_path", lambda: self._join_path_retry_hint(question, sql)),
            *(() if locked_projection_columns is not None or authoritative_projection else (
                ("projection", lambda: self._projection_conflict(question, sql)),
            )),
            ("parallel_measures", lambda: self._parallel_measures_retry_hint(question, sql)),
            ("row_grain", lambda: self._row_grain_retry_hint(question, sql)),
            ("set_semantics", lambda: self._set_semantics_retry_hint(question, sql)),
            ("numeric_result_type", lambda: self._numeric_result_type_retry_hint(question, sql)),
            ("date_storage", lambda: self._date_storage_retry_hint(sql)),
            ("enum_literal_case", lambda: self._enum_literal_case_retry_hint(sql)),
            (
                "wildcard_literal_broadening",
                lambda: self._wildcard_literal_retry_hint(question, sql),
            ),
        )
        for code, check in checks:
            evidence = check()
            if isinstance(evidence, QuerySemanticConflict):
                return evidence
            if evidence:
                return QuerySemanticConflict(
                    code=code,
                    message=str(evidence),
                    constraints={"must_fix": str(evidence)},
                )
        return None

    def _semantic_retry_hint(self, question: str, sql: str) -> str:
        """兼容字符串诊断；权威依据为类型化冲突对象。"""
        conflict = self._semantic_conflict(question, sql)
        return conflict.message if conflict else ""

    def _join_path_retry_hint(self, question: str, sql: str) -> str:
        """Validate resolvable JOIN edges against declared direct FK facts.

        The existing relation gate proves that the *set of referenced tables*
        is connected somewhere in the schema graph.  That does not prove that
        a candidate's actual ``JOIN ... ON`` columns follow that graph.  This
        bounded pass only acts when aliases and equality columns are simple and
        the two tables have at least one fully declared direct FK.  Incomplete
        metadata, CTEs, set operations and expression joins remain untouched.
        """
        code = _sql_code_only(sql, mask_identifiers=False)
        if not re.search(r"\bJOIN\b", code, re.IGNORECASE) or re.match(
            r"\s*WITH\b", code, re.IGNORECASE,
        ) or re.search(r"\b(?:UNION|INTERSECT|EXCEPT)\b", code, re.IGNORECASE):
            return ""

        tables_by_folded = {name.casefold(): name for name in self.schema.tables}
        aliases: Dict[str, str] = {}
        source_pattern = re.compile(
            r"\b(?:FROM|JOIN)\s+(?P<table>[A-Za-z_][\w$]*)"
            r"(?:\s+(?:AS\s+)?(?P<alias>[A-Za-z_][\w$]*))?",
            re.IGNORECASE,
        )
        keywords = {
            "cross", "full", "group", "having", "inner", "join", "left", "limit",
            "on", "order", "right", "where",
        }
        for source in source_pattern.finditer(code):
            table_name = tables_by_folded.get(source.group("table").casefold())
            if not table_name:
                continue
            aliases[source.group("table").casefold()] = table_name
            alias = source.group("alias")
            if alias and alias.casefold() not in keywords:
                aliases[alias.casefold()] = table_name

        explicit_pairs = {
            (
                left_table.casefold(), left_column.casefold(),
                right_table.casefold(), right_column.casefold(),
            )
            for left_table, left_column, right_table, right_column
            in SchemaRelationAnalyzer._EXPLICIT_RELATION_RE.findall(question or "")
        }
        explicit_pairs |= {
            (right_table, right_column, left_table, left_column)
            for left_table, left_column, right_table, right_column in list(explicit_pairs)
        }

        join_pattern = re.compile(
            r"\bJOIN\s+(?P<table>[A-Za-z_][\w$]*)"
            r"(?:\s+(?:AS\s+)?(?P<alias>(?!ON\b|WHERE\b|JOIN\b|GROUP\b|"
            r"ORDER\b|HAVING\b|LIMIT\b)[A-Za-z_][\w$]*))?\s+ON\s+"
            r"(?P<condition>.*?)(?=\b(?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\b|"
            r"\bWHERE\b|\bGROUP\s+BY\b|\bHAVING\b|\bORDER\s+BY\b|\bLIMIT\b|$)",
            re.IGNORECASE | re.DOTALL,
        )
        equality_pattern = re.compile(
            r"(?P<left_alias>[A-Za-z_][\w$]*)\s*\.\s*"
            r"(?P<left_column>[A-Za-z_][\w$]*)\s*=\s*"
            r"(?P<right_alias>[A-Za-z_][\w$]*)\s*\.\s*"
            r"(?P<right_column>[A-Za-z_][\w$]*)",
            re.IGNORECASE,
        )

        for join in join_pattern.finditer(code):
            joined_table = tables_by_folded.get(join.group("table").casefold())
            if not joined_table:
                continue
            joined_alias = (join.group("alias") or join.group("table")).casefold()
            if joined_alias in keywords:
                joined_alias = join.group("table").casefold()

            actual_edges: List[tuple[str, str, str, str]] = []
            for equality in equality_pattern.finditer(join.group("condition")):
                left_alias = equality.group("left_alias").casefold()
                right_alias = equality.group("right_alias").casefold()
                left_table = aliases.get(left_alias)
                right_table = aliases.get(right_alias)
                if not left_table or not right_table or left_table == right_table:
                    continue
                if joined_alias not in {left_alias, right_alias}:
                    continue
                actual_edges.append((
                    left_table, equality.group("left_column"),
                    right_table, equality.group("right_column"),
                ))
            if not actual_edges:
                continue

            for left_table, _left_column, right_table, _right_column in actual_edges:
                declared_edges: List[tuple[str, str, str, str]] = []
                for table in self.schema.tables.values():
                    for column in table.columns:
                        if not column.fk_table or not column.fk_column:
                            continue
                        if {table.name.casefold(), column.fk_table.casefold()} != {
                            left_table.casefold(), right_table.casefold(),
                        }:
                            continue
                        declared_edges.append((
                            table.name, column.name, column.fk_table, column.fk_column,
                        ))
                if not declared_edges:
                    continue

                actual_for_pair = [
                    edge for edge in actual_edges
                    if {edge[0].casefold(), edge[2].casefold()} == {
                        left_table.casefold(), right_table.casefold(),
                    }
                ]
                matches_declared = any(
                    (
                        actual[0].casefold(), actual[1].casefold(),
                        actual[2].casefold(), actual[3].casefold(),
                    ) in {
                        (
                            declared[0].casefold(), declared[1].casefold(),
                            declared[2].casefold(), declared[3].casefold(),
                        ),
                        (
                            declared[2].casefold(), declared[3].casefold(),
                            declared[0].casefold(), declared[1].casefold(),
                        ),
                    }
                    for actual in actual_for_pair
                    for declared in declared_edges
                )
                matches_explicit = any(
                    (
                        actual[0].casefold(), actual[1].casefold(),
                        actual[2].casefold(), actual[3].casefold(),
                    ) in explicit_pairs
                    for actual in actual_for_pair
                )
                if matches_declared or matches_explicit:
                    continue

                actual_text = "、".join(
                    f"{edge[0]}.{edge[1]} = {edge[2]}.{edge[3]}"
                    for edge in actual_for_pair
                )
                declared_text = " 或 ".join(
                    f"{edge[0]}.{edge[1]} = {edge[2]}.{edge[3]}"
                    for edge in declared_edges
                )
                return (
                    "候选 SQL 的实际 JOIN 边（" + actual_text
                    + "）没有使用 schema 已声明的直接外键（" + declared_text
                    + "）。表集合虽然在关系图上连通，但 ON 列仍会连接错误记录；"
                    "请按已声明外键修正 JOIN。"
                )
        return ""

    _ZH_NAME_RE = re.compile(r"^(name_?zh|name_?cn|chinese_?name|中文名)$", re.IGNORECASE)
    _EN_NAME_RE = re.compile(r"^(name_?en|name_?eng|english_?name|en_?name)$", re.IGNORECASE)

    def _person_name_hint(self, allowed_tables: Optional[List[str]]) -> str:
        """同时存在中文名/英文名列时提示双列匹配（英文名列常大量缺失）。"""
        allowed = set(allowed_tables) if allowed_tables is not None else None
        for tname, tbl in self.schema.tables.items():
            if allowed is not None and tname not in allowed:
                continue
            names = [c.name for c in tbl.columns]
            zh = next((n for n in names if self._ZH_NAME_RE.fullmatch(n)), None)
            en = next((n for n in names if self._EN_NAME_RE.fullmatch(n)), None)
            if zh and en:
                return (
                    f"人名匹配提示：{tname} 同时有中文名 {tname}.{zh} 与英文名 {tname}.{en}，"
                    f"英文名列可能大量缺失。按人名筛选时应同时尝试两列"
                    f"（如 {zh} LIKE ? OR {en} LIKE ?），不要只查其中一列。"
                )
        return ""

    def _knowledge_topic_hint(self, allowed_tables: Optional[List[str]]) -> str:
        """存在论文/知识文档类标题正文列时，提示主题检索应查询真实列。"""
        allowed = set(allowed_tables) if allowed_tables is not None else None
        for tname, tbl in self.schema.tables.items():
            if allowed is not None and tname not in allowed:
                continue
            names = {c.name.casefold(): c.name for c in tbl.columns}
            title = names.get("title")
            body = names.get("body") or names.get("abstract")
            if title and body:
                return (
                    f"主题检索提示：{tname} 的 {tname}.{title}/{tname}.{body} 保存论文或文档标题与正文/摘要"
                    "（中英文混存）。用户按主题找论文/资料/文档时，应查询这些列"
                    "（LIKE 包含匹配，可对多个同义关键词取 OR），不要因为检索片段不足就回答“找不到”。"
                )
        return ""

    _PAY_TIMESTAMP_NAME_RE = re.compile(
        r"(paid|payment|pay)[_@]?(at|time|date|on)", re.IGNORECASE,
    )

    def _payment_status_hint(self, allowed_tables: Optional[List[str]]) -> str:
        """状态枚举含 paid 且存在可空支付时间戳时，锚定“已支付”的状态口径。

        消除模型用“支付时间戳非空”替代“状态=paid”的口径漂移（已退款/已取消
        记录可能保留支付时间）。条件基于真实 schema 与值域抽样，不绑定关键词。
        """
        allowed = set(allowed_tables) if allowed_tables is not None else None
        for tname, tbl in self.schema.tables.items():
            if allowed is not None and tname not in allowed:
                continue
            status_col = next(
                (c for c in tbl.columns if c.name.casefold() == "status"), None,
            )
            pay_col = next(
                (c for c in tbl.columns if self._PAY_TIMESTAMP_NAME_RE.fullmatch(c.name)),
                None,
            )
            if status_col is None or pay_col is None:
                continue
            sampled = {
                str(v).strip().casefold() for v in status_col.sample_values if v is not None
            }
            if "paid" not in sampled:
                continue
            return (
                f"支付口径提示：{tname}.{status_col.name} 是业务状态枚举（取值含 paid），"
                f"{tname}.{pay_col.name} 是可空支付时间戳。用户说“已支付/已付款/已支付订单”"
                "默认指状态口径（status='paid'），不要用支付时间戳非空替代——"
                "已退款/已取消的记录也可能保留支付时间。仅当用户明确询问"
                "“有支付时间的”或“支付过（不论后续状态）”时才使用时间戳非空口径。"
            )
        return ""

    def _validate_allowed_tables(self, sql: str, allowed_tables: List[str]) -> None:
        """保证拆分查询只读取规划器分配的表，阻止模型跨分支越界。"""
        allowed = {name.casefold() for name in allowed_tables if name in self.schema.tables}
        if len(allowed) != 1:
            raise NL2SQLError("独立查询分支必须绑定且只能绑定一张合法目标表")
        inspected = re.sub(r"/\*.*?\*/", " ", sql or "", flags=re.DOTALL)
        inspected = re.sub(r"--[^\r\n]*", " ", inspected)
        inspected = re.sub(r"'(?:''|[^'])*'", "''", inspected)
        if re.match(r"\s*WITH\b", inspected, re.IGNORECASE) or re.search(r"\bJOIN\b", inspected, re.IGNORECASE):
            raise NL2SQLError("独立查询分支不允许 CTE 或 JOIN")
        from_re = re.compile(
            r"\bFROM\s+(?:ONLY\s+)?(?:\"([^\"]+)\"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))",
            re.IGNORECASE,
        )
        sources = [next(value for value in match.groups() if value is not None) for match in from_re.finditer(inspected)]
        if len(sources) != 1:
            raise NL2SQLError("独立查询分支必须且只能包含一个 FROM 数据源")
        source = sources[0].casefold()
        if source not in allowed:
            raise NL2SQLError(f"查询分支越界引用表: {sources[0]}")
        from_match = next(from_re.finditer(inspected))
        from_tail = inspected[from_match.end():]
        from_clause = re.split(
            r"\b(?:WHERE|GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT|UNION|INTERSECT|EXCEPT)\b",
            from_tail,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        if "," in from_clause:
            raise NL2SQLError("独立查询分支不允许多表 FROM")

    _SQL_SOURCE_RE = re.compile(
        r"\b(?:FROM|JOIN)\s+(?:ONLY\s+)?"
        r"(?:\"([^\"]+)\"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))",
        re.IGNORECASE,
    )

    def _sql_referenced_tables(self, sql: str) -> List[str]:
        """提取 SQL 真实引用的物理表（屏蔽字符串/注释，排除 CTE 名称）。"""
        inspected = re.sub(r"/\*.*?\*/", " ", sql or "", flags=re.DOTALL)
        inspected = re.sub(r"--[^\r\n]*", " ", inspected)
        inspected = re.sub(r"'(?:''|[^'])*'", "''", inspected)
        cte_names = set(re.findall(
            r"\bWITH\s+(?:RECURSIVE\s+)?(\w+)", inspected, re.IGNORECASE,
        ))
        cte_names |= set(re.findall(r",\s*(\w+)\s+AS\s*\(", inspected, re.IGNORECASE))
        by_folded = {name.casefold(): name for name in self.schema.tables}
        found: List[str] = []
        for match in self._SQL_SOURCE_RE.finditer(inspected):
            raw = next(value for value in match.groups() if value is not None)
            folded = raw.casefold()
            if folded in cte_names or folded not in by_folded:
                continue
            if by_folded[folded] not in found:
                found.append(by_folded[folded])
        return found

    def _relation_clarification(self, sql: str, question: str) -> Optional[DBAnswer]:
        """跨表 SQL 缺少已声明 FK/显式关系时转为澄清，补齐 compose 通道的关系门禁。

        规划层的 table_relationship 门禁只在按表名识别出多张目标表时生效；
        纯中文语义问题识别不出目标表时，模型可能自行用同名列对齐 CTE 绕过
        预检。此处在执行前对 SQL 实际引用的物理表复用同一关系分析器，
        保证两条通道执行同一规则（ADR-010/ADR-011）。
        """
        tables = self._sql_referenced_tables(sql)
        if len(tables) < 2:
            return None
        result = SchemaRelationAnalyzer(self.schema).analyze(tables, question)
        if result["connected"]:
            return None
        prompt = (
            "生成的 SQL 需要跨表关联，但这些表之间没有已声明的外键路径。"
            "请提供明确等值关联，例如 orders.customer_id = customers.id。"
        )
        return DBAnswer(
            kind="clarification",
            narrative=f"为了避免猜测或误操作，我还不能执行。请先补充表关联条件。",
            clarification={
                "missing": "table_relationship",
                "missing_label": "表关联条件",
                "question": prompt,
                "candidates": [],
                "input_hint": "可以直接回复补充内容，也可以重新输入一条完整指令。",
                "original_question": question,
            },
            steps=[{
                "tool": "relation_gate",
                "status": "needs_clarification",
                "missing": "table_relationship",
                "tables": tables,
            }],
        )

    def answer(
        self,
        question: str,
        history: Optional[list] = None,
        allowed_tables: Optional[List[str]] = None,
    ) -> DBAnswer:
        self.last_generated_sql = ""
        self.last_candidate_sql = ""
        self.last_semantic_hint = ""
        self.semantic_repair_count = 0
        self.last_query_intent = QueryIntentContract()
        self.last_relational_contract = self._compile_relational_contract(question)
        self.last_relational_plan = None
        self.last_candidate_search = None
        if self.last_relational_contract.ambiguities:
            ambiguity = self.last_relational_contract.ambiguities[0]
            choices = [str(item) for item in ambiguity.get("choices") or []]
            values = " 或 ".join(
                str(item) for item in ambiguity.get("category_values") or []
            )
            prompt = (
                f"“{ambiguity.get('modifier')}”是只作用于 {values} 中后一类，"
                "还是同时作用于两类？"
            )
            return DBAnswer(
                kind="clarification",
                narrative="这个查询存在会改变结果的布尔条件作用域歧义，需要先确认口径。",
                clarification={
                    "missing": "boolean_filter_scope",
                    "missing_label": "筛选条件作用范围",
                    "question": prompt,
                    "candidates": choices,
                    "input_hint": "请选择一个口径，或用括号关系重新描述完整条件。",
                    "original_question": question,
                },
                steps=[{
                    "tool": "relational_algebra_contract",
                    "version": self.last_relational_contract.version,
                    "status": "needs_clarification",
                    "contract": self.last_relational_contract.as_dict(),
                }],
            )
        native_plan = self._compile_grouped_metrics_plan(
            question, self.last_relational_contract, allowed_tables,
        )
        if native_plan is None:
            native_plan = self._compile_native_relational_plan(
                question, self.last_relational_contract, allowed_tables,
            )
        if native_plan is not None:
            try:
                if isinstance(native_plan, RelationalGroupedMetricsPlan):
                    native_sql = RelationalGroupedMetricsRenderer(
                        self.schema, native_plan.dialect,
                    ).render(native_plan)
                else:
                    native_sql = SQLiteRelationalPlanRenderer(self.schema).render(native_plan)
                if allowed_tables:
                    self._validate_allowed_tables(native_sql, allowed_tables)
            except (NL2SQLError, ValueError):
                native_sql = ""
            if native_sql:
                relation_block = (
                    None if isinstance(native_plan, RelationalSetQueryPlan)
                    else self._relation_clarification(native_sql, question)
                )
                native_conflict = (
                    None if isinstance(native_plan, RelationalGroupedMetricsPlan)
                    else self._semantic_conflict(question, native_sql)
                )
                if relation_block is not None:
                    return relation_block
                if native_conflict is None:
                    native_result = self.security.execute(native_sql)
                    if not native_result.error:
                        self.last_relational_plan = native_plan
                        self.last_candidate_sql = native_sql
                        self.last_generated_sql = native_sql
                        if isinstance(native_plan, RelationalScalarAggregatePlan):
                            local_columns = [native_plan.output_name]
                        elif isinstance(native_plan, RelationalSetQueryPlan):
                            local_columns = [native_plan.output_name]
                        else:
                            local_columns = [
                                _COL_ZH.get(name, _COL_ZH.get(name.casefold(), name))
                                for name in native_result.columns
                            ]
                        return DBAnswer(
                            kind="query",
                            narrative="查询完成（由本地关系计划确定性执行）",
                            sql=native_result.sql,
                            columns=local_columns,
                            rows=native_result.rows,
                            relational_plan=native_plan.as_dict(),
                            steps=[{
                                "tool": "native_relational_planner",
                                "version": native_plan.version,
                                "status": "compiled_and_executed",
                                "dialect": native_plan.dialect,
                                "model_calls": 0,
                                "plan": native_plan.as_dict(),
                            }, {
                                "tool": "relational_algebra_contract",
                                "version": self.last_relational_contract.version,
                                "status": "validated",
                                "contract": self.last_relational_contract.as_dict(),
                            }],
                        )
        schema_txt = self._schema_context(question, allowed_tables)
        if self.last_relational_contract.is_actionable():
            schema_txt += (
                "\n\n本地独立关系代数合同（由问句、schema 和显式业务字典确定，必须逐项实现；"
                "不要额外输出仅用于筛选、分组或排序的列）：\n"
                + json.dumps(
                    self.last_relational_contract.as_dict(),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
        max_retry = 2
        semantic_retry_count = 0
        bad_sql = ""
        last_error: Optional[str] = None
        candidate_search_step: Optional[dict] = None

        connector = getattr(self.security, "connector", None)
        dialect = str(getattr(connector, "dialect", "sqlite") or "sqlite").lower()
        dialect_name = {
            "sqlite": "SQLite",
            "mysql": "MySQL",
            "postgresql": "PostgreSQL",
        }.get(dialect, dialect)
        for attempt in range(max_retry + 1):  # 首次 + 2 次自纠错
            if attempt == 0:
                prompt = self._SYSTEM_PROMPT.format(
                    dialect_name=dialect_name,
                    schema=schema_txt,
                    question=question,
                )
            else:
                prompt = self._RETRY_PROMPT.format(
                    schema=schema_txt, question=question,
                    bad_sql=bad_sql, error=last_error or "",
                )
            try:
                obj = _llm_ask_json(prompt, self.llm_cfg, history=history)
            except LLMServiceError as e:
                # 模型网关已完成网络重试；余额、鉴权、配置和终态传输错误
                # 再做整轮 NL2SQL 自纠错不会改变结果，直接准确上报。
                return DBAnswer(kind="error", narrative=str(e), error=str(e))
            except DBAgentError as e:
                if attempt >= max_retry:
                    return DBAnswer(kind="error", narrative=str(e), error=str(e))
                last_error = str(e)
                continue
            sql = (obj.get("sql") or "").strip()
            summary = (obj.get("summary_zh") or "").strip()
            declared_intent = QueryIntentContract.from_payload(obj.get("intent"))
            if declared_intent.is_declared():
                self.last_query_intent = declared_intent
            if not sql:
                last_error = "LLM 未返回 SQL"
                if attempt >= max_retry:
                    return DBAnswer(kind="error", narrative=last_error, error=last_error)
                continue
            self.last_candidate_sql = sql
            if allowed_tables:
                try:
                    self._validate_allowed_tables(sql, allowed_tables)
                except NL2SQLError as exc:
                    bad_sql = sql
                    last_error = str(exc)
                    if attempt >= max_retry:
                        return DBAnswer(
                            kind="error",
                            narrative=f"SQL 执行失败（已尝试 {attempt + 1} 次）：{exc}",
                            sql=sql,
                            error=str(exc),
                        )
                    continue
            relation_block = self._relation_clarification(sql, question)
            if relation_block is not None:
                # 缺少可验证关系不是生成错误，重试无法修复，直接转澄清
                return relation_block
            semantic_conflict = self._semantic_conflict(question, sql)
            semantic_hint = semantic_conflict.message if semantic_conflict else ""
            self.last_semantic_hint = semantic_hint
            if semantic_conflict:
                search = self._try_local_contract_repair(
                    question=question,
                    bad_sql=sql,
                    conflict=semantic_conflict,
                    allowed_tables=allowed_tables,
                )
                if search is None:
                    readiness = self._candidate_search_readiness(
                        question, semantic_conflict,
                    )
                    if not readiness["ready"]:
                        candidate_search_step = {
                            "tool": "bounded_candidate_search",
                            "version": "1.1",
                            "status": "incomplete_semantic_contract",
                            "requested_max": 0,
                            "received_count": 0,
                            "eligible_count": 0,
                            "selected_candidate_id": None,
                            "selection_basis": "fail_closed_before_model_search",
                            "candidate_protocol": "not_started",
                            "model_calls": 0,
                            "contract_coverage": readiness["coverage"],
                            "missing_dimensions": readiness["missing_dimensions"],
                            "assessments": [],
                        }
                        self.last_candidate_search = candidate_search_step
                        return DBAnswer(
                            kind="clarification",
                            narrative=(
                                "本地检查已发现候选查询与问句冲突，但当前独立"
                                "语义合同还不足以安全裁决新候选，因此没有再次调用模型或执行 SQL。"
                            ),
                            clarification={
                                "missing": "query_semantics",
                                "missing_label": "查询输出与粒度",
                                "question": semantic_hint,
                                "candidates": [],
                                "input_hint": "请明确要返回哪些列，以及是一行一个明细、一个分组还是一个汇总值。",
                                "original_question": question,
                            },
                            steps=[candidate_search_step, {
                                "tool": "query_contract",
                                "status": "needs_clarification",
                                "missing": "query_semantics",
                                "relational_contract": self.last_relational_contract.as_dict(),
                            }],
                        )
                    if semantic_retry_count >= 1 or attempt >= max_retry:
                        search = None
                    else:
                        semantic_retry_count += 1
                        self.semantic_repair_count = semantic_retry_count
                        try:
                            search = self._search_semantic_repair_candidates(
                                question=question,
                                schema_txt=schema_txt,
                                bad_sql=sql,
                                semantic_conflict=semantic_conflict,
                                history=history,
                                allowed_tables=allowed_tables,
                            )
                        except LLMServiceError as exc:
                            return DBAnswer(kind="error", narrative=str(exc), error=str(exc))
                        except DBAgentError as exc:
                            return DBAnswer(kind="error", narrative=str(exc), error=str(exc))
                if search is not None:
                    candidate_search_step = search["diagnostic"]
                    selected = search["selected"]
                    if selected is not None:
                        sql = str(selected["sql"] or "").strip()
                        summary = str(selected["summary_zh"] or "").strip()
                        selected_intent = selected["intent"]
                        if selected_intent.is_declared():
                            self.last_query_intent = selected_intent
                        self.last_candidate_sql = sql
                        self.last_semantic_hint = ""
                    else:
                        rejection_hints = [
                            str(item.get("detail") or "")
                            for item in candidate_search_step.get("assessments") or []
                            if item.get("reason_code") == "semantic_contract"
                            and str(item.get("detail") or "")
                        ]
                        self.last_semantic_hint = rejection_hints[0] if rejection_hints else semantic_hint
                        return DBAnswer(
                            kind="clarification",
                            narrative=(
                                "有界候选搜索仍未找到可由本地证据唯一选定的查询，"
                                "因此没有执行。请补充你希望的输出列或每行代表的对象。"
                            ),
                            clarification={
                                "missing": "query_semantics",
                                "missing_label": "查询输出与粒度",
                                "question": self.last_semantic_hint,
                                "candidates": [],
                                "input_hint": "请明确要返回哪些列，以及是一行一个明细、一个分组还是一个汇总值。",
                                "original_question": question,
                            },
                            steps=[candidate_search_step, {
                                "tool": "query_contract",
                                "status": "needs_clarification",
                                "missing": "query_semantics",
                                "relational_contract": self.last_relational_contract.as_dict(),
                            }],
                        )
                else:
                    return DBAnswer(
                        kind="clarification",
                        narrative=(
                            "候选查询虽然可以执行，但输出或统计口径仍与问题存在可验证冲突，"
                            "因此没有执行。请补充你希望的输出列或每行代表的对象。"
                        ),
                        clarification={
                            "missing": "query_semantics",
                            "missing_label": "查询输出与粒度",
                            "question": semantic_hint,
                            "candidates": [],
                            "input_hint": "请明确要返回哪些列，以及是一行一个明细、一个分组还是一个汇总值。",
                            "original_question": question,
                        },
                        steps=[{
                            "tool": "query_contract",
                            "status": "needs_clarification",
                            "missing": "query_semantics",
                            "relational_contract": self.last_relational_contract.as_dict(),
                        }],
                    )
            res = self.security.execute(sql)
            bad_sql = sql  # noqa: F841
            if res.error:
                last_error = res.error
                candidate_search_step = None
                if attempt >= max_retry:
                    return DBAnswer(
                        kind="error",
                        narrative=f"SQL 执行失败（已尝试 {attempt + 1} 次）：{res.error}",
                        sql=sql, error=res.error,
                    )
                continue
            accepted_sql = sql
            accepted_result = res
            accepted_summary = summary
            review_step: Optional[dict] = None
            if self._requires_contract_review(question, sql, allowed_tables, res):
                review = self._review_candidate(question, schema_txt, sql, res)
                contract = review["contract"]
                review_step = {
                    "tool": "query_contract_review",
                    "version": contract.version,
                    "status": review["status"],
                    "decision": review["decision"],
                    "reason_code": review["reason_code"],
                    "intent": contract.as_dict(),
                    "candidate_changed": False,
                }
                revised_sql = str(review["sql"] or "").strip()
                if review["decision"] == "revise" and revised_sql != sql:
                    if allowed_tables:
                        try:
                            self._validate_allowed_tables(revised_sql, allowed_tables)
                        except NL2SQLError:
                            review_step["status"] = "revision_rejected_scope"
                            revised_sql = ""
                    if revised_sql:
                        relation_block = self._relation_clarification(revised_sql, question)
                        if relation_block is not None:
                            relation_block.steps.insert(0, review_step)
                            return relation_block
                        revision_hint = self._semantic_retry_hint(question, revised_sql)
                        if revision_hint:
                            review_step["status"] = "revision_rejected_local_contract"
                            review_step["reason_code"] = "local_semantic_contract"
                            revised_sql = ""
                    if revised_sql:
                        revised_result = self.security.execute(revised_sql)
                        if revised_result.error:
                            review_step["status"] = "revision_execution_failed"
                        else:
                            accepted_sql = revised_sql
                            accepted_result = revised_result
                            accepted_summary = review["summary_zh"] or summary
                            review_step["status"] = "revision_accepted"
                            review_step["candidate_changed"] = True
            # 成功：组装 DBAnswer
            self.last_generated_sql = accepted_sql
            narrative = accepted_summary or "查询完成"
            if accepted_result.truncated:
                narrative += (
                    f"（结果较多，仅展示前 {len(accepted_result.rows)} 行，"
                    f"共 {accepted_result.row_count} 行）"
                )
            answer_steps: List[dict] = []
            if self.last_query_intent.is_declared():
                answer_steps.append({
                    "tool": "query_intent_contract",
                    "version": self.last_query_intent.version,
                    "status": "declared",
                    "intent": self.last_query_intent.as_dict(),
                })
            if candidate_search_step is not None:
                answer_steps.append(candidate_search_step)
            if review_step is not None:
                answer_steps.append(review_step)
            if self.last_relational_contract.is_declared():
                answer_steps.append({
                    "tool": "relational_algebra_contract",
                    "version": self.last_relational_contract.version,
                    "status": "validated",
                    "contract": self.last_relational_contract.as_dict(),
                })
            return DBAnswer(
                kind="query", narrative=narrative, sql=accepted_result.sql,
                columns=[_col_zh(c, self.llm_cfg) for c in accepted_result.columns],
                rows=accepted_result.rows,
                steps=answer_steps,
            )
        # 理论不可达
        return DBAnswer(kind="error", narrative="NL2SQL 执行异常", error="NL2SQL 执行异常")


class NL2WriteExecutor:
    """自然语言 → 写 SQL → WriteSecurity 校验 → dry-run 预览 → WriteProposal（不落库）。

    只产出"待确认提案"，绝不直接执行；用户批准由 DBAgent.confirm_write 完成。
    """

    _SYSTEM_PROMPT = (
        "你是 sqlite 数据库操作助手。根据下面的数据库结构，把用户的操作意图翻译成一条写 SQL。\n"
        "规则：\n"
        "1. 只生成单条写 SQL（INSERT/UPDATE/DELETE/CREATE TABLE/ALTER TABLE/DROP TABLE 之一），"
        "严禁输出多条语句，严禁用分号分隔多条 SQL，sql 字段内不得包含 SELECT/注释/空语句\n"
        "2. UPDATE/DELETE 必须带 WHERE 条件，禁止无界更新/删除；批量修改用一条 UPDATE 配合 WHERE 完成\n"
        "3. 表名/列名必须与 schema 完全一致；条件值优先使用 schema 中给出的抽样值\n"
        "4. 结果只输出一个 JSON 对象：{{\"sql\": \"...\", \"summary_zh\": \"一句话中文操作说明\"}}\n"
        "5. sql 字段内不要用 markdown 围栏\n\n"
        "数据库结构：\n{schema}\n\n"
        "用户操作：{question}\n\n"
        "输出 JSON："
    )

    def __init__(self, connector: DBConnector, schema: SchemaSnapshot,
                 security: WriteSecurity, previewer: WritePreviewer,
                 llm_cfg: str = "default"):
        self.connector = connector
        self.schema = schema
        self.security = security
        self.previewer = previewer
        self.llm_cfg = llm_cfg

    def prepare(self, question: str, history: Optional[list] = None) -> DBAnswer:
        """生成写提案（dry-run 预览后返回 kind='write_pending'，零落库）。"""
        schema_txt = self.schema.compact()
        prompt = self._SYSTEM_PROMPT.format(schema=schema_txt, question=question)
        obj = _llm_ask_json(prompt, self.llm_cfg, history=history)
        sql = (obj.get("sql") or "").strip()
        summary = (obj.get("summary_zh") or "").strip()
        if not sql:
            return DBAnswer(kind="error", narrative="LLM 未返回写 SQL", error="LLM 未返回写 SQL")
        # 多语句兜底：LLM 偶尔输出带分号/多条（含全角分号），sqlite execute 只允许单条 → 折叠为第一条有效写语句
        folded = self._collapse_single_stmt(sql)
        if folded != sql:
            sql = folded
            summary = f"{summary}（自动折叠为单条语句）" if summary else "自动折叠为单条语句"
        return _prepare_write_proposal(
            self.connector, self.security, self.previewer, sql, summary,
        )

    @staticmethod
    def _collapse_single_stmt(sql: str) -> str:
        """折叠为单条写语句：分号/换行/连续无分隔多条 → 取第一条有效写语句。"""
        s = (sql or "").strip().replace("；", ";")
        parts = [p.strip() for p in s.split(";") if p.strip()]
        if len(parts) > 1:
            return NL2WriteExecutor._pick_first_write(parts)
        if len(parts) == 1:
            s = parts[0].strip()  # 去掉尾分号，保持单条干净
        # 无分号但多行多条（LLM 偶用换行分隔多条语句）
        lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
        starts = [i for i, ln in enumerate(lines)
                  if NL2WriteExecutor._is_stmt_start(ln)]
        if len(starts) > 1:
            return lines[starts[0]]
        # 连续无分隔多条：'...' UPDATE / id=2 UPDATE 等（REPLACE 函数调用不误切）
        cut = NL2WriteExecutor._find_second_stmt_start(s)
        if cut is not None:
            return s[:cut].strip().rstrip(";").strip()
        return s

    @staticmethod
    def _find_second_stmt_start(s: str):
        """定位第二条语句起始位置；前一个非空白字符须为语句结束符（;/引号/括号/数字/字母）。"""
        import re  # noqa: PLC0415
        pat = re.compile(r"\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE)\b", re.I)
        matches = list(pat.finditer(s))
        if len(matches) < 2:
            return None
        for m in matches[1:]:
            cut = m.start()
            prev = ""
            for ch in reversed(s[:cut]):
                if not ch.isspace():
                    prev = ch
                    break
            if prev in ";)'\"}" or prev.isalnum():
                return cut
        return None

    @staticmethod
    def _pick_first_write(parts: list) -> str:
        for p in parts:
            if NL2WriteExecutor._is_stmt_start(p):
                return p.rstrip(";").strip()
        return parts[0].rstrip(";").strip()

    @staticmethod
    def _is_stmt_start(ln: str) -> bool:
        return ln.lstrip().upper().startswith(
            ("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "REPLACE", "SELECT"))

    @staticmethod
    def _default_summary(kind: str, table: str) -> str:
        table = table or "数据表"
        return {
            "INSERT": f"向 {table} 插入新记录",
            "UPDATE": f"更新 {table} 中的记录",
            "DELETE": f"删除 {table} 中的记录",
            "CREATE": f"创建 {table}",
            "ALTER": f"修改 {table} 结构",
            "DROP": f"删除 {table}（不可恢复！）",
        }.get(kind, f"对 {table} 执行写操作")


# ---------------------------------------------------------------------------
# RagRetriever —— 表/列语义 + 值域 → 召回 → LLM 组织（步骤5填充）
# ---------------------------------------------------------------------------

class RagRetriever:
    """Retrieve bounded database evidence and organize it into an answer."""

    _STOPWORDS = {"的", "了", "吗", "呢", "啊", "是", "有", "也", "在", "和", "与", "或",
                  "多少", "哪些", "什么", "怎么", "如何", "请问", "帮我", "一下", "这个", "那个",
                  "一个", "可以", "列出", "介绍", "关于", "信息", "数据", "请问"}
    _ORGANIZE_PROMPT = (
        "你是数据库内容问答助手。下面是从 sqlite 数据库检索到的记录片段（证据），请根据证据用中文回答用户问题。\n"
        "规则：\n"
        "1. 只依据证据回答；证据不足以回答时如实说明，不要编造数据\n"
        "2. 可适当总结/归纳多条证据\n"
        "3. 只输出一个 JSON 对象：{{\"answer_zh\": \"回答内容\"}}\n\n"
        "证据：\n{evidence}\n\n"
        "用户问题：{question}\n\n"
        "输出 JSON："
    )
    _OVERVIEW_QUESTION_RE = re.compile(
        r"(这个库|该库|这个数据库|该数据库|当前数据库|数据库).{0,24}"
        r"(业务|作用|用途|干什么|做什么|场景|简介|概览|介绍|说明)|"
        r"(业务|库|表结构).{0,8}(背景|定位|概览|总结)",
        re.IGNORECASE,
    )
    _OVERVIEW_PROMPT = (
        "你是 sqlite 数据库助手。仅根据下面的数据库结构（表名、字段、行数），"
        "用中文简要说明这个数据库大概承载什么业务、核心表各自的作用和主要关联。\n"
        "规则：\n"
        "1. 只依据给定结构归纳，不要编造结构中不存在的表或字段\n"
        "2. 不要生成或执行 SQL，不引用任何具体数据行\n"
        "3. 回答不超过 200 字\n"
        "4. 只输出一个 JSON 对象：{{\"answer_zh\": \"...\"}}\n\n"
        "数据库结构：\n{schema}\n\n"
        "输出 JSON："
    )

    def __init__(self, connector: DBConnector, schema: SchemaSnapshot, llm_cfg: str = "default",
                 max_evidence: int = 15, max_like_queries: int = 400,
                 allowed_tables: Optional[List[str]] = None,
                 allowed_columns: Optional[Dict[str, List[str]]] = None,
                 row_filters: Optional[Dict[str, List[dict]]] = None):
        self.connector = connector
        self.schema = schema
        self.llm_cfg = llm_cfg
        self.max_evidence = max_evidence
        self.max_like_queries = max_like_queries
        self.allowed_tables = (
            frozenset(str(name).casefold() for name in allowed_tables)
            if allowed_tables is not None else None
        )
        self.allowed_columns = _normalize_column_scope(allowed_columns)
        if self.allowed_columns and self.allowed_tables is None:
            raise ValueError("字段级授权必须建立在显式表级授权之上")
        self.row_filters = _normalize_row_scope(row_filters)
        if self.row_filters and self.allowed_tables is None:
            raise ValueError("行级授权必须建立在显式表级授权之上")

    # -- 关键词提取（引号术语优先，中文 2-gram 补足，去停用词） --
    _QUOTED_TERM_RE = re.compile(r"[“\"『「']([^”\"』」']{1,40})[”\"』」']")

    def _keywords(self, question: str, limit: int = 8) -> List[str]:
        out, seen = [], set()

        def _add(token: str) -> None:
            if token in seen or len(token) < 2:
                return
            if any(s in token for s in self._STOPWORDS):
                return
            seen.add(token)
            out.append(token)

        # 引号内术语是用户显式指定的检索目标，优先于位置性 n-gram 召回
        for span in self._QUOTED_TERM_RE.findall(question or ""):
            for token in re.findall(r"[\w\u4e00-\u9fff]+", span):
                if len(out) >= limit:
                    break
                _add(token)
        chunks = re.findall(r"[\u4e00-\u9fff]{2,}", question)
        grams = []
        for ch in chunks:
            for i in range(len(ch) - 1):
                grams.append(ch[i:i + 2])
            for i in range(len(ch) - 2):
                grams.append(ch[i:i + 3])
        for g in grams:
            if len(out) >= limit:
                break
            _add(g)
        return out

    def _escape_like(self, kw: str) -> str:
        return kw.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    # -- 召回：FTS/LIKE 统一走 LIKE 扫描（小库友好；FTS 表同样支持列 LIKE） --
    @staticmethod
    def _text_scan_columns(tbl) -> List[str]:
        """优先扫描真正的文本列；避免前 3 个非 BLOB 列恰好是 id/日期时漏掉备注等 TEXT 列。"""
        declared_text = []
        fallback = []
        for column in tbl.columns:
            ctype = (column.type or "").upper()
            if "BLOB" in ctype:
                continue
            if "TEXT" in ctype or "CHAR" in ctype or "CLOB" in ctype or not ctype.strip():
                declared_text.append(column.name)
            else:
                fallback.append(column.name)
        return (declared_text + fallback)[:3]

    def _recall(self, keywords: List[str]) -> List[dict]:
        evidence: List[dict] = []
        conn = self.connector.connect()
        try:
            row_internal = _prepare_sqlite_row_views(
                conn, self.row_filters, self.allowed_columns,
            ) if self.row_filters else {}
            _install_sqlite_scope_authorizer(
                conn,
                allowed_tables=self.allowed_tables,
                allowed_columns=self.allowed_columns,
                row_internal_columns=row_internal,
                allow_writes=False,
                unavailable_error="当前连接无法安全执行表/字段级授权检索",
            )
            cur = conn.cursor()
            budget = self.max_like_queries
            for tname, tbl in self.schema.tables.items():
                if len(evidence) >= self.max_evidence or budget <= 0:
                    break
                # 优先取 3 个文本列参与 LIKE（BLOB 列跳过）
                text_cols = self._text_scan_columns(tbl)
                if not text_cols:
                    continue
                col_names = [c.name for c in tbl.columns]
                projection = ", ".join(
                    '"' + name.replace('"', '""') + '"' for name in col_names
                )
                for col in text_cols:
                    if len(evidence) >= self.max_evidence or budget <= 0:
                        break
                    for kw in keywords:
                        if len(evidence) >= self.max_evidence or budget <= 0:
                            break
                        budget -= 1
                        try:
                            sql = (
                                f'SELECT {projection} FROM "{tname}" '
                                f'WHERE "{col}" LIKE ? ESCAPE "\\" LIMIT 3'
                            )
                            cur.execute(sql, (f"%{self._escape_like(kw)}%",))
                            for row in cur.fetchall():
                                evidence.append({
                                    "table": tname,
                                    "columns": col_names,
                                    "row": [str(v)[:40] if not isinstance(v, bytes) else f"<blob {len(v)}B>" for v in row],
                                    "matched": kw,
                                })
                        except sqlite3.Error:
                            continue
            return evidence[: self.max_evidence]
        finally:
            self.connector.close(conn)

    def _business_overview(self, question: str, history: Optional[list]) -> Optional[DBAnswer]:
        """记录检索空召回且问题指向库级业务定位时，从 schema 元数据生成概览。

        只读元数据 → LLM 叙述，不执行 SQL、不读取数据行；LLM 失败保持原
        “未找到相关内容”的诚实回答（fail-safe，不编造）。
        """
        if self.schema is None or not self._OVERVIEW_QUESTION_RE.search(question or ""):
            return None
        try:
            obj = _llm_ask_json(
                self._OVERVIEW_PROMPT.format(schema=self.schema.compact()),
                self.llm_cfg,
                history=history,
            )
            narrative = (obj.get("answer_zh") or "").strip()
        except DBAgentError:
            return None
        if not narrative:
            return None
        return DBAnswer(
            kind="retrieve",
            narrative=narrative,
            evidence=[],
            steps=[{"tool": "schema_overview", "status": "executed"}],
        )

    def answer(self, question: str, history: Optional[list] = None) -> DBAnswer:
        kws = self._keywords(question)
        ev = self._recall(kws) if kws else []
        if not ev:
            overview = self._business_overview(question, history)
            if overview is not None:
                return overview
            return DBAnswer(kind="retrieve", narrative="未在数据库中找到与问题相关的内容。", evidence=[])
        ev_txt = "\n".join(
            f"[{i}] 表={e['table']}, 列=[{','.join(e['columns'])}], 值=[{' | '.join(e['row'])}], 命中={e['matched']}"
            for i, e in enumerate(ev, 1)
        )
        prompt = self._ORGANIZE_PROMPT.format(evidence=ev_txt, question=question)
        try:
            obj = _llm_ask_json(prompt, self.llm_cfg, history=history)
            narrative = (obj.get("answer_zh") or "").strip() or "已检索到相关内容（详见证据）。"
        except DBAgentError as e:
            narrative = f"检索到 {len(ev)} 条相关记录，但 LLM 组织回答失败：{e}"
        return DBAnswer(kind="retrieve", narrative=narrative, evidence=ev)


class SchemaRelationAnalyzer:
    """分析目标表能否通过已声明 FK 或用户显式等值条件连通。"""

    _EXPLICIT_RELATION_RE = re.compile(
        r"[`\"]?([\w$]+)[`\"]?\s*\.\s*[`\"]?([\w$]+)[`\"]?\s*=\s*"
        r"[`\"]?([\w$]+)[`\"]?\s*\.\s*[`\"]?([\w$]+)[`\"]?",
        re.IGNORECASE,
    )

    def __init__(self, schema: SchemaSnapshot):
        self.schema = schema

    def _valid_column(self, table_name: str, column_name: str) -> bool:
        table = self.schema.tables.get(table_name)
        return bool(table and any(column.name == column_name for column in table.columns))

    def analyze(self, tables: List[str], question: str = "") -> dict:
        targets = list(dict.fromkeys(tables))
        missing = [name for name in targets if name not in self.schema.tables]
        if missing:
            return {
                "summary": "跨表关系预检发现不存在的表: " + ", ".join(missing),
                "tables": targets,
                "connected": False,
                "edges": [],
                "paths": [],
                "invalid_tables": missing,
            }

        adjacency: Dict[str, List[str]] = {name: [] for name in self.schema.tables}
        edges: List[dict] = []

        def add_edge(left_table: str, right_table: str, edge: dict) -> None:
            if right_table not in adjacency[left_table]:
                adjacency[left_table].append(right_table)
            if left_table not in adjacency[right_table]:
                adjacency[right_table].append(left_table)
            if edge not in edges:
                edges.append(edge)

        for table in self.schema.tables.values():
            for column in table.columns:
                if not column.fk_table or column.fk_table not in self.schema.tables:
                    continue
                add_edge(table.name, column.fk_table, {
                    "from": f"{table.name}.{column.name}",
                    "to": f"{column.fk_table}.{column.fk_column or ''}".rstrip("."),
                    "source": "foreign_key",
                })

        for match in self._EXPLICIT_RELATION_RE.finditer(question or ""):
            left_table, left_column, right_table, right_column = match.groups()
            if left_table == right_table:
                continue
            if not self._valid_column(left_table, left_column) \
                    or not self._valid_column(right_table, right_column):
                continue
            add_edge(left_table, right_table, {
                "from": f"{left_table}.{left_column}",
                "to": f"{right_table}.{right_column}",
                "source": "explicit",
            })

        paths: List[list[str]] = []
        connected = True
        if targets:
            anchor = targets[0]
            for target in targets[1:]:
                queue: List[tuple[str, list[str]]] = [(anchor, [anchor])]
                visited = {anchor}
                found: List[str] = []
                while queue:
                    current, path = queue.pop(0)
                    if current == target:
                        found = path
                        break
                    for neighbor in adjacency.get(current, []):
                        if neighbor not in visited:
                            visited.add(neighbor)
                            queue.append((neighbor, [*path, neighbor]))
                if not found:
                    connected = False
                    paths.append([anchor, target])
                else:
                    paths.append(found)
        summary = (
            f"{len(targets)} 张目标表可通过已声明或显式关系连接"
            if connected else
            "目标表之间未发现完整的已声明或显式关联路径"
        )
        return {
            "summary": summary,
            "tables": targets,
            "connected": connected,
            "edges": edges,
            "paths": paths,
        }


# ---------------------------------------------------------------------------
# NaturalLanguageDatabasePlanner —— 统一 NL-to-Database 操作语义层
# ---------------------------------------------------------------------------

class NaturalLanguageDatabasePlanner:
    """把自然语言意图归一为可审计的自研数据库操作计划。"""

    _SCHEMA_DETAIL_RE = re.compile(
        r"(表结构|字段|字段名|列名|有哪些列|有哪些字段|结构是什么|介绍.{0,8}表|"
        r"columns?|describe|desc\s+|table\s+structure)",
        re.IGNORECASE,
    )
    _SCHEMA_OVERVIEW_RE = re.compile(
        r"(有哪些表|所有表|全部表|列出.{0,8}表|显示.{0,8}表|每张表|数据库结构|"
        r"show\s+tables|list\s+(all\s+)?tables|database\s+schema)",
        re.IGNORECASE,
    )
    _VALUE_DOMAIN_STATS_RE = re.compile(
        r"(取值|值域|枚举值|distinct\s*值|有哪些值|值的?分布|每个值|各(?:种)?值)",
        re.IGNORECASE,
    )
    _RELATION_QUESTION_RE = re.compile(r"(关系|关联|外键)", re.IGNORECASE)
    _RELATION_STATS_RE = re.compile(
        r"(统计|多少|数量|金额|计算|总和|平均|趋势|比较|差异|count|sum|avg)",
        re.IGNORECASE,
    )
    _GENERIC_SINGLE_TABLE_QUERY_RE = re.compile(
        r"(一共|总共|总计|总共有)?.{0,8}(多少条|多少行|记录数|行数)|"
        r"(前|最近)\s*\d+\s*(条|行).{0,8}(数据|记录)|"
        r"\b(count|row\s+count|first\s+\d+\s+rows?)\b",
        re.IGNORECASE,
    )
    _NEW_REQUEST_RE = re.compile(
        r"^(查询|查找|统计|计算|列出|展示|显示|介绍|删除|删掉|新增|插入|添加|创建|"
        r"更新|修改|设置|写入|有哪些表|一共|总共|多少|哪个|哪些|谁|什么|怎么|如何|为什么|"
        r"show\s+tables|select|update|delete|insert|create|alter|drop)",
        re.IGNORECASE,
    )
    _COMPLETE_WRITE_RE = re.compile(
        r"^(把|将).{1,160}(改成|改为|设为|设置成|设置为|更新为|删除|删掉|移除)|"
        r"^(请|帮我|麻烦)?\s*(新增|插入|添加|创建|更新|修改|删除).{2,160}",
        re.IGNORECASE,
    )
    _USER_CORRECTION_RE = re.compile(
        r"^\s*(不对|不是|错了|算错|搞错|理解错|口径不对|不正确|不应该)[，,。！!]",
    )
    _RERUN_REQUEST_RE = re.compile(r"(重新|再)(计算|统计|算|查)")
    _CLEAR_NEW_READ_RE = re.compile(
        r"^(查询|查找|统计|计算|列出|展示|显示|介绍|有哪些表|一共|总共|多少|哪个|"
        r"哪些|谁|什么|怎么|如何|为什么|show\s+tables|select)",
        re.IGNORECASE,
    )
    _DERIVED_METRIC_RE = re.compile(
        r"(转化率|留存率|复购率|客单价|增长率|利润率|毛利率|达成率|完成率|成功率|失败率|"
        r"conversion\s+rate|retention\s+rate|growth\s+rate|profit\s+margin)",
        re.IGNORECASE,
    )
    _FIELD_AGGREGATE_RE = re.compile(
        r"(平均|均值|合计|总额|总和|求和|最大(?:值)?|最小(?:值)?|"
        r"\b(?:sum|avg|average|max|min)\b)",
        re.IGNORECASE,
    )
    _TIME_SIGNAL_RE = re.compile(
        r"(最近|近期|近来|过去|本期|当前周期|趋势|同比|环比|按日|按周|按月|按季度|按年|"
        r"财年|财季|会计年度|会计季度|工作日|交易日|营业日|"
        r"日期|时间|\bdate\b|\btime\b|\btrend\b|year[- ]over[- ]year|month[- ]over[- ]month)",
        re.IGNORECASE,
    )
    _BUSINESS_CALENDAR_RE = re.compile(
        r"(财年|财季|会计年度|会计季度|工作日|交易日|营业日|"
        r"fiscal\s+(?:year|quarter)|business\s+days?|trading\s+days?)",
        re.IGNORECASE,
    )
    _VAGUE_TIME_RE = re.compile(
        r"(近期|近来|最近(?:一段时间)?|过去(?:一段时间)?|本期|当前周期|趋势|同比|环比)",
        re.IGNORECASE,
    )
    _EXPLICIT_TIME_RE = re.compile(
        r"\d{4}\s*(?:[-/.年])\s*\d{1,2}(?:\s*(?:[-/.月])\s*\d{1,2})?|"
        r"\d{4}\s*(?:财年|会计年度)(?:\s*(?:第\s*)?[1-4一二三四]\s*(?:季度|财季)|\s*[Qq]\s*[1-4])?|"
        r"(?:最近|近|过去)\s*\d+\s*(?:天|日|周|个月|月|季度|年)|"
        r"(?:今天|昨日|昨天|本周|上周|本月|上月|本季度|上季度|今年|去年)|"
        r"\b\d{4}-\d{2}-\d{2}\b",
        re.IGNORECASE,
    )
    _TIME_GRAIN_ANALYSIS_RE = re.compile(
        r"(趋势|走势|变化|同比|环比|\btrend\b|year[- ]over[- ]year|month[- ]over[- ]month)",
        re.IGNORECASE,
    )
    _EXPLICIT_TIME_GRAIN_RE = re.compile(
        r"((?:按|每)(?:日|天|周|月|季度|季|年)|(?:日|周|月|季|年)度|"
        r"\b(?:daily|weekly|monthly|quarterly|yearly)\b)",
        re.IGNORECASE,
    )

    def __init__(self, schema: SchemaSnapshot, semantic_catalog: Optional[SemanticCatalog] = None):
        self.schema = schema
        self.semantic_catalog = semantic_catalog or SemanticCatalog(schema)

    def _semantic_matches(self, question: str) -> List[dict]:
        return self.semantic_catalog.resolve(question).matches

    def _target_tables(self, question: str) -> List[str]:
        q = question.casefold()
        targets = []
        for name in self.schema.tables:
            folded = name.casefold()
            if re.fullmatch(r"[a-z0-9_]+", folded):
                matched = re.search(rf"(?<![a-z0-9_]){re.escape(folded)}(?![a-z0-9_])", q)
            else:
                matched = folded in q
            if matched:
                targets.append(name)
        for match in self._semantic_matches(question):
            name = match.get("table")
            if name in self.schema.tables and name not in targets:
                targets.append(name)
        return targets

    def llm_map_target_table(self, question: str, schema_context: str, llm_cfg: str = "default") -> Optional[str]:
        """规则层未命中目标表时，让 LLM 从真实 schema 表名中消歧业务对象。

        - LLM 只能从 schema 现有表名中选择，不得发明表名；
        - 输出必须通过 schema 校验，只接受唯一且存在的表；
        - LLM 不可用/解析失败/无法确定时返回 None（调用方回退澄清，fail-closed）。
        """
        if not self.schema.tables:
            return None
        tables = sorted(self.schema.tables)
        table_lines = "\n".join(
            f"- {name}: {', '.join(col.name for col in self.schema.tables[name].columns[:12])}"
            for name in tables
        )
        prompt = (
            "用户请求中提到的业务对象（例如客户、订单、商品）应映射到数据库哪张表？\n"
            "只能从下面真实表清单中选择，不得发明表名；无法确定时返回 null。\n"
            f"表清单：\n{table_lines}\n\n"
            "只输出 JSON：{\"table\": \"<真实表名>\"} 或 {\"table\": null}\n"
            f"用户请求：{question}"
        )
        try:
            obj = _llm_ask_json(prompt, llm_cfg)
        except Exception:  # noqa: BLE001 —— LLM 不可用时回退澄清
            return None
        raw = str(obj.get("table") or "").strip()
        if not raw or raw.lower() == "null":
            return None
        folded = {name.casefold(): name for name in tables}
        return folded.get(raw.casefold())

    def llm_map_target_columns(self, question: str, table_name: str, llm_cfg: str = "default") -> List[dict]:
        """规则层未命中目标列时，让 LLM 从真实表字段中消歧业务字段。

        返回 ``[{"term": "姓名", "column": "name"}, ...]`` 映射对，column 必须是真实字段；
        LLM 不可用/无法确定时返回空列表（fail-closed）。
        """
        table = self.schema.tables.get(table_name)
        if table is None or not table.columns:
            return []
        columns = sorted(col.name for col in table.columns)
        prompt = (
            f"用户请求中提到的业务字段（例如姓名、城市、金额）应映射到表 {table_name} 的哪些字段？\n"
            "只能从下面真实字段清单中选择，不得发明字段名；无法确定时返回空数组。\n"
            f"字段清单：{', '.join(columns)}\n\n"
            "只输出 JSON：{\"columns\": [{\"term\": \"用户原文中的业务词\", \"column\": \"真实字段名\"}, ...]}\n"
            "或 {\"columns\": []}\n"
            f"用户请求：{question}"
        )
        try:
            obj = _llm_ask_json(prompt, llm_cfg)
        except Exception:  # noqa: BLE001 —— LLM 不可用时回退澄清
            return []
        raw = obj.get("columns") or []
        if not isinstance(raw, list):
            return []
        folded = {name.casefold(): name for name in columns}
        result = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            term = str(item.get("term") or "").strip()
            column = folded.get(str(item.get("column") or "").strip().casefold())
            if term and column and {"term": term, "column": column} not in result:
                result.append({"term": term, "column": column})
        return result

    def llm_rewrite_write_request(self, question: str, llm_cfg: str = "default") -> Optional[str]:
        """把自由表述的写请求解析为结构化 JSON，再确定性拼装为规范指令。

        - 覆盖“把张三的城市改成北京”（缺对象词、中文字段名）与
          “删除客户张三”（实体名作筛选条件）两类规则层无法消解的形态；
        - 表名/字段名只接受真实 schema 名称，值只接受用户原话字面值；
          update/delete 必须带筛选条件（与 WriteSecurity 的 WHERE 边界一致）；
        - 结构化解析 + 固定模板拼装避免自由文本改写的格式漂移；任何校验
          失败返回 None（调用方保持原澄清，fail-closed）。改写后仍走完整
          歧义校验、dry-run 预览与显式确认，不放宽任何写边界。
        """
        tables = sorted(self.schema.tables)
        if not tables:
            return None
        folded_tables = {name.casefold(): name for name in tables}
        table_lines = "\n".join(
            f"- {name}: {', '.join(col.name for col in self.schema.tables[name].columns[:12])}"
            for name in tables
        )
        prompt = (
            "解析用户的自然语言数据库写请求。表名和字段名只能使用下方清单中的真实名称。\n"
            "只输出 JSON（不要多余文字）：\n"
            '{"operation": "update|delete|insert", "table": "<真实表名>", '
            '"set": [{"column": "<真实字段>", "value": "<字面值>"}], '
            '"where": [{"column": "<真实字段>", "value": "<字面值>"}]}\n'
            "规则：update 的 set 与 where 必填；delete 的 where 必填；insert 用 set 列出全部字段与值；"
            "值一律取用户原话中的字面值（数字/日期保持原样），不得编造或翻译字段名；"
            "无法确定表名、字段名或条件时输出 {\"result\": null}。\n"
            "示例：表 customers(id,name,city)，请求“把张三的城市改成北京”→\n"
            '{"operation": "update", "table": "customers", '
            '"set": [{"column": "city", "value": "北京"}], '
            '"where": [{"column": "name", "value": "张三"}]}\n\n'
            f"表结构：\n{table_lines}\n\n"
            f"用户请求：{question}"
        )
        try:
            obj = _llm_ask_json(prompt, llm_cfg)
        except Exception:  # noqa: BLE001 —— LLM 不可用时保持原澄清
            return None
        if not isinstance(obj, dict):
            return None
        if "result" in obj and obj.get("result") is None:
            return None  # LLM 明确表示无法确定
        if "operation" not in obj:
            return None
        table = folded_tables.get(str(obj.get("table") or "").strip().casefold())
        if table is None:
            return None
        operation = str(obj.get("operation") or "").strip().lower()
        if operation not in ("update", "delete", "insert"):
            return None
        folded_columns = {
            col.name.casefold(): col.name for col in self.schema.tables[table].columns
        }

        def _pairs(raw) -> List[tuple]:
            pairs = []
            for item in raw or []:
                if not isinstance(item, dict):
                    continue
                column = folded_columns.get(
                    str(item.get("column") or "").strip().casefold(),
                )
                value = str(item.get("value") or "").strip().strip("“”\"'，,。；;")
                if column and value:
                    pairs.append((column, value))
            return pairs

        set_pairs = _pairs(obj.get("set"))
        where_pairs = _pairs(obj.get("where"))
        if operation in ("update", "insert") and not set_pairs:
            return None
        if operation in ("update", "delete") and not where_pairs:
            return None
        where_txt = " 且 ".join(f"{c} = {v}" for c, v in where_pairs)
        if operation == "delete":
            return f"删除 {table} 中 {where_txt} 的记录"
        if operation == "insert":
            fields = ", ".join(f"{c}={v}" for c, v in set_pairs)
            return f"新增 {table}：{fields}"
        set_txt = "，".join(f"{c} 设为 {v}" for c, v in set_pairs)
        return f"更新 {table} 中 {set_txt}，筛选条件：{where_txt}"

    def rewrite_with_mapped_columns(self, question: str, table_name: str, mapped_columns: List[dict]) -> str:
        """用 LLM 映射对把问题中的中文业务字段归一化为 ``列名=值``。

        对每个 ``{term, column}``：优先匹配 “term + 引号/冒号/等号/为/是 + 值” 的常见
        写法并改写为 ``column=值``；无值上下文只替换裸词。改写是尽力而为，
        不改变问题其余内容；LLM 不可用时调用方直接跳过。
        """
        if not mapped_columns:
            return question
        rewritten = question
        for item in mapped_columns:
            term = re.escape(str(item.get("term") or ""))
            column = str(item.get("column") or "")
            if not term or not column:
                continue
            patterns = [
                # “城市改成北京/设为北京”等动词连接：先吃掉动词再取值（须在通用模式前）
                rf"{term}\s*(?:改成|改为|设为|设置为|设置成|更新为|修改为|调成|变更为)\s*"
                rf"[“\"']?([^“\"'，,。；;]{{1,40}})[“\"']?",
                rf"{term}\s*[“\"']?([^“\"'，,。；;的之]{{1,40}})[“\"']?",
                rf"{term}\s*[:：]\s*([^，,。；;]{{1,40}})",
                rf"{term}\s*[=＝]\s*([^，,。；;]{{1,40}})",
                rf"{term}\s+(?:为|是)\s+([^，,。；;]{{1,40}})",
            ]
            replaced = False
            for pattern in patterns:
                match = re.search(pattern, rewritten, re.IGNORECASE)
                if match:
                    value = match.group(1).strip().strip("“”\"'，,。；;")
                    if value and value not in ("改成", "改为", "设为", "设置为", "设置成", "更新为", "修改为", "调成", "变更为"):
                        rewritten = (
                            rewritten[:match.start()]
                            + f"{column}={value}"
                            + rewritten[match.end():]
                        )
                        replaced = True
                        break
            if not replaced:
                rewritten = re.sub(
                    rf"(?<![A-Za-z0-9_]){term}(?![A-Za-z0-9_])",
                    column, rewritten, flags=re.IGNORECASE,
                )
        return rewritten

    def _is_exact_table_reference(self, question: str, table_name: str) -> bool:
        compact = question.strip().casefold()
        if compact in {table_name.casefold(), f"{table_name}表".casefold(), f"表{table_name}".casefold()}:
            return True
        return any(
            match.get("kind") == "table_alias"
            and match.get("table") == table_name
            and compact in {
                str(match.get("term") or "").casefold(),
                f"{match.get('term')}表".casefold(),
                f"表{match.get('term')}".casefold(),
            }
            for match in self._semantic_matches(question)
        )

    @staticmethod
    def _name_matches(text: str, name: str) -> bool:
        folded = name.casefold()
        if re.fullmatch(r"[a-z0-9_]+", folded):
            return bool(re.search(rf"(?<![a-z0-9_]){re.escape(folded)}(?![a-z0-9_])", text.casefold()))
        return folded in text.casefold()

    def _target_columns(self, question: str, tables: List[str]) -> List[str]:
        found: List[str] = []
        for table_name in tables:
            table = self.schema.tables.get(table_name)
            if table is None:
                continue
            for column in table.columns:
                if self._name_matches(question, column.name) and column.name not in found:
                    found.append(column.name)
        for match in self._semantic_matches(question):
            column_name = match.get("column")
            if match.get("table") in tables and column_name and column_name not in found:
                found.append(column_name)
        return found

    def _update_target_columns(self, question: str, tables: List[str]) -> List[str]:
        """只识别赋值侧字段，避免把 WHERE 条件字段误认为修改目标。"""
        found: List[str] = []
        for table_name in tables:
            table = self.schema.tables.get(table_name)
            if table is None:
                continue
            for column in table.columns:
                name = re.escape(column.name)
                patterns = (
                    rf"(?:目标字段\s*[:：]\s*)[`\"]?{name}[`\"]?",
                    rf"[`\"]?{name}[`\"]?\s*(?:改成|改为|设为|设置成|设置为|变更为|调成|更新为|修改为)",
                    rf"(?:更新|修改)\s*[`\"]?{name}[`\"]?\s*(?:为|成)",
                    rf"\bset\s+[`\"]?{name}[`\"]?\s*=",
                )
                if any(re.search(pattern, question, re.IGNORECASE) for pattern in patterns):
                    found.append(column.name)
        for match in self._semantic_matches(question):
            if match.get("kind") != "column_alias" or match.get("table") not in tables:
                continue
            term = re.escape(str(match.get("term") or ""))
            patterns = (
                rf"(?:目标字段\s*[:：]\s*){term}",
                rf"{term}\s*(?:改成|改为|设为|设置成|设置为|变更为|调成|更新为|修改为)",
                rf"(?:更新|修改)\s*{term}\s*(?:为|成)",
            )
            column_name = str(match.get("column") or "")
            if column_name and any(re.search(pattern, question, re.IGNORECASE) for pattern in patterns) \
                    and column_name not in found:
                found.append(column_name)
        return found

    @staticmethod
    def _new_object_name(question: str) -> str:
        patterns = (
            r"\bcreate\s+(?:table|index|view)\s+[`\"]?([a-z_][\w$]*)",
            r"对象名称\s*[:：]\s*[`\"]?([\w$\u4e00-\u9fff]+)",
            r"(?:创建|新建|建立|建)\s*(?:一个|一张|一份)?\s*(?:名为\s*)?[`\"]?([a-zA-Z_][\w$]*)[`\"]?\s*(?:表|索引|视图)",
            r"(?:创建|新建|建立|建)\s*(?:一个|一张|一份)?\s*(?:名为\s*)?[`\"]?([\w$\u4e00-\u9fff]+?)[`\"]?\s*(?:表|索引|视图)",
        )
        for pattern in patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                return match.group(1)
        return ""

    @staticmethod
    def _has_new_value(question: str) -> bool:
        return bool(re.search(
            r"(改成|改为|设为|设置成|设置为|变更为|调成|更新为)\s*[^，。；,;\s]+|"
            r"(更新|修改).{1,36}(为|成)\s*[^，。；,;\s]+|"
            r"\bset\s+[`\"\w$]+\s*=\s*[^,;\s]+|"
            r"修改后的值\s*[:：]\s*[^，。；,;\s]+",
            question,
            re.IGNORECASE,
        ))

    def _has_filter_condition(self, question: str, tables: List[str], action: str) -> bool:
        if re.search(
            r"\bwhere\s+\S+|筛选条件\s*[:：]\s*\S+|"
            r"(?:主键|编号)\s*(?:=|==|为|是|等于|大于|小于|不等于|:|：)\s*\S+",
            question,
            re.I,
        ):
            return True
        columns = self._target_columns(question, tables)
        for match in self._semantic_matches(question):
            if match.get("table") not in tables:
                continue
            if match.get("kind") == "enum_value":
                return True
            if match.get("kind") == "column_alias":
                term = re.escape(str(match.get("term") or ""))
                if re.search(
                    rf"{term}\s*(?:>=|<=|!=|<>|=|==|>|<|为|是|等于|大于|小于|不等于|:|：)\s*[^，。；,;\s]+",
                    question,
                    re.I,
                ):
                    return True
        for table_name in tables:
            table = self.schema.tables.get(table_name)
            if table is None:
                continue
            primary_keys = {column.name for column in table.columns if column.pk}
            for name in columns:
                predicate = (
                    rf"{re.escape(name)}\s*(?:>=|<=|!=|<>|=|==|>|<|为|是|等于|大于|小于|不等于|:|：)"
                    rf"\s*[^，。；,;\s]+"
                )
                if name in primary_keys and re.search(predicate, question, re.I):
                    return True
                if re.search(predicate, question, re.I):
                    return True
        return False

    @staticmethod
    def _is_numeric_column(column: DBColumn) -> bool:
        return bool(re.search(
            r"(INT|REAL|NUMERIC|DECIMAL|FLOAT|DOUBLE|NUMBER)",
            column.type or "",
            re.IGNORECASE,
        )) and not column.pk and not column.fk_table

    @staticmethod
    def _is_time_column(column: DBColumn) -> bool:
        return is_time_column(column)

    def _candidate_columns(self, tables: List[str], predicate) -> List[tuple[str, DBColumn]]:
        candidates: List[tuple[str, DBColumn]] = []
        for table_name in tables:
            table = self.schema.tables.get(table_name)
            if table is None:
                continue
            for column in table.columns:
                if predicate(column):
                    candidates.append((table_name, column))
        return candidates

    def _has_defined_metric(self, question: str) -> bool:
        return any(
            match.get("kind") in {"metric", "ratio_metric"}
            for match in self._semantic_matches(question)
        )

    def _has_defined_business_calendar(self, question: str) -> bool:
        return any(
            match.get("kind") == "business_calendar"
            for match in self._semantic_matches(question)
        )

    @staticmethod
    def _has_metric_definition(question: str) -> bool:
        match = re.search(r"指标口径\s*[:：]\s*(\S.{2,})", question, re.IGNORECASE)
        return bool(match)

    @staticmethod
    def _has_business_calendar_definition(question: str) -> bool:
        match = re.search(r"业务日历\s*[:：]\s*(\S.{2,})", question, re.IGNORECASE)
        return bool(match)

    def _mentioned_candidates(
        self,
        question: str,
        candidates: List[tuple[str, DBColumn]],
    ) -> List[tuple[str, DBColumn]]:
        mentioned = []
        semantic_columns = {
            (str(match.get("table") or ""), str(match.get("column") or ""))
            for match in self._semantic_matches(question)
            if match.get("column")
        }
        for table_name, column in candidates:
            qualified = f"{table_name}.{column.name}"
            if self._name_matches(question, qualified) or self._name_matches(question, column.name) \
                    or (table_name, column.name) in semantic_columns:
                mentioned.append((table_name, column))
        return mentioned

    def _has_explicit_time_range(self, question: str) -> bool:
        if self._EXPLICIT_TIME_RE.search(question):
            return True
        match = re.search(r"时间范围\s*[:：]\s*([^；;，,。]+)", question, re.IGNORECASE)
        return bool(match and self._EXPLICIT_TIME_RE.search(match.group(1)))

    def _has_explicit_time_grain(self, question: str) -> bool:
        if self._EXPLICIT_TIME_GRAIN_RE.search(question):
            return True
        match = re.search(r"时间粒度\s*[:：]\s*([^；;，,。]+)", question, re.IGNORECASE)
        return bool(match and self._EXPLICIT_TIME_GRAIN_RE.search("按" + match.group(1)))

    def _configured_time_grain(
        self,
        question: str,
        time_candidates: List[tuple[str, DBColumn]],
        mentioned_time: List[tuple[str, DBColumn]],
    ) -> str:
        matched = {
            str(item.get("default_grain") or "")
            for item in self._semantic_matches(question)
            if item.get("kind") == "time_field" and item.get("default_grain")
        }
        if len(matched) == 1:
            return next(iter(matched))
        selected = mentioned_time or (time_candidates if len(time_candidates) == 1 else [])
        selected_keys = {(table_name, column.name) for table_name, column in selected}
        grains = {
            str(entry.get("default_grain") or "")
            for entry in self.semantic_catalog.entries
            if entry.get("kind") == "time_field"
            and entry.get("default_grain")
            and (entry.get("table"), entry.get("column")) in selected_keys
        }
        return next(iter(grains)) if len(grains) == 1 else ""

    def resolve_followup(
        self,
        question: str,
        history: Optional[list],
        clarification: Optional[dict] = None,
    ) -> str:
        """把对澄清问题的短回复并回原指令；完整的新请求保持原样。"""
        if (not history and not clarification) or len(question) > 200 \
                or self._CLEAR_NEW_READ_RE.search(question.strip()) \
                or self._COMPLETE_WRITE_RE.search(question.strip()) \
                or re.search(
                    r"(目标表|对象名称|目标字段|筛选条件|修改后的值|新增内容|字段定义|结构变更定义|"
                    r"关联条件|指标口径|聚合字段|时间字段|时间范围|时间粒度|维度层级|业务日历|"
                    r"布尔筛选作用域)\s*[:：]",
                    question,
                ):
            return question
        if clarification:
            original = str(clarification.get("original_question") or "").strip()
            missing = str(clarification.get("missing") or "").strip()
            labels = {
                "target_table": "目标表",
                "object_name": "对象名称",
                "target_field": "目标字段",
                "new_value": "修改后的值",
                "filter_condition": "筛选条件",
                "record_values": "新增内容",
                "operation_type": "操作类型",
                "object_definition": "字段定义",
                "schema_change_definition": "结构变更定义",
                "table_relationship": "关联条件",
                "metric_definition": "指标口径",
                "aggregation_field": "聚合字段",
                "time_field": "时间字段",
                "time_range": "时间范围",
                "time_grain": "时间粒度",
                "business_calendar": "业务日历",
                "dimension_level": "维度层级",
                "boolean_filter_scope": "布尔筛选作用域",
            }
            if original and missing in labels:
                return f"{original.rstrip('？?。；;')}；{labels[missing]}：{question.strip()}"
        if self._NEW_REQUEST_RE.search(question.strip()):
            return question
        last_assistant = next(
            (str(item.get("content") or "") for item in reversed(history) if item.get("role") == "assistant"),
            "",
        )
        previous_user = next(
            (str(item.get("content") or "") for item in reversed(history) if item.get("role") == "user"),
            "",
        )
        if not last_assistant or not previous_user:
            return question
        stripped = question.strip()
        if previous_user and (
            self._USER_CORRECTION_RE.match(stripped)
            or (self._RERUN_REQUEST_RE.search(stripped) and len(stripped) <= 60)
        ):
            # 写确认链路有自己的合并与复检语义，不在读路径改写
            if "预览" in last_assistant or "确认" in last_assistant:
                return question
            body = self._USER_CORRECTION_RE.sub("", stripped).strip("，,。！! ") or stripped
            return f"{previous_user.rstrip('？?。；;')}；指标口径：{body}"
        if "请先补充目标表" in last_assistant:
            matched = self._target_tables(question)
            if len(matched) == 1 and self._is_exact_table_reference(question, matched[0]):
                return f"{previous_user.rstrip('？?。；;')}；目标表：{matched[0]}"
        if "请先补充筛选条件" in last_assistant:
            return f"{previous_user.rstrip('？?。；;')}；筛选条件：{question.strip()}"
        if "请先补充目标字段" in last_assistant:
            return f"{previous_user.rstrip('？?。；;')}；目标字段：{question.strip()}"
        if "请先补充修改后的值" in last_assistant:
            return f"{previous_user.rstrip('？?。；;')}；修改后的值：{question.strip()}"
        if "请先补充新增记录的字段和值" in last_assistant:
            return f"{previous_user.rstrip('？?。；;')}；新增内容：{question.strip()}"
        return question

    def plan_schema(self, question: str) -> Optional[DatabaseOperationPlan]:
        """识别无需 LLM 的结构查看操作；未命中则交给通用意图路由。"""
        targets = self._target_tables(question)
        clarification_field = bool(re.search(
            r"(?:目标字段|聚合字段|时间字段|时间粒度|维度层级|字段定义|结构变更定义)\s*[:：]",
            question,
            re.IGNORECASE,
        ))
        schema_change = bool(
            re.search(
                r"(?:字段|列|column).{0,40}(?:增加|新增|添加|加|删除|修改|重命名|add|drop|alter|rename)|"
                r"(?:增加|新增|添加|加|删除|修改|重命名|add|drop|alter|rename).{0,40}(?:字段|列|column)",
                question,
                re.IGNORECASE,
            )
        )
        if targets and self._SCHEMA_DETAIL_RE.search(question) \
                and not schema_change and not clarification_field \
                and not self._VALUE_DOMAIN_STATS_RE.search(question):
            return DatabaseOperationPlan(
                action="inspect_table",
                mode="read",
                intent="schema",
                target_tables=targets,
                risk="low",
                status="planned",
                confidence=1.0,
                reasoning="命中表名和结构/字段查看表达，可直接读取数据库元数据",
            )
        if self._SCHEMA_OVERVIEW_RE.search(question) \
                and not self._VALUE_DOMAIN_STATS_RE.search(question):
            return DatabaseOperationPlan(
                action="inspect_schema",
                mode="read",
                intent="schema",
                target_tables=targets,
                risk="low",
                status="planned",
                confidence=1.0,
                reasoning="命中数据库表清单/结构查看表达，可直接读取数据库元数据",
            )
        if len(targets) >= 2 and self._RELATION_QUESTION_RE.search(question) \
                and not self._RELATION_STATS_RE.search(question) \
                and not schema_change and not clarification_field:
            return DatabaseOperationPlan(
                action="inspect_relations",
                mode="read",
                intent="schema",
                target_tables=targets,
                risk="low",
                status="planned",
                confidence=1.0,
                reasoning="命中表间关系查看表达，可直接读取外键元数据",
            )
        return None

    def from_intent(self, question: str, result: IntentResult) -> DatabaseOperationPlan:
        targets = self._target_tables(question)
        action = {
            "query": "select",
            "retrieve": "retrieve",
            "compose": "analyze",
            "write": "mutate",
        }.get(result.intent, "retrieve")
        mode = "write" if result.intent == "write" else "read"
        risk = "medium" if mode == "write" else "low"
        reasoning = result.reasoning

        if result.intent == "write":
            if not targets and result.target_table:
                actual_by_folded = {
                    name.casefold(): name for name in self.schema.tables
                }
                model_target = actual_by_folded.get(result.target_table.casefold())
                if model_target:
                    targets = [model_target]
                else:
                    reasoning = (
                        f"{reasoning or ''}；模型建议的目标表未通过真实 schema 校验，已忽略"
                    ).strip("；")
            compact = question.casefold()
            row_delete = bool(re.search(
                r"(表中|表内|表里|记录|数据|行|\brows?\b|\brecords?\b|\bfrom\b|\bwhere\b|\bid\s*=)",
                compact,
            ))
            if not row_delete and re.search(r"(drop|删除|删掉|移除).{0,20}(表|索引|视图|table|index|view)(?!(?:.{0,24}(?:字段|列|column)))", compact):
                action, risk = "drop", "high"
            elif re.search(r"(create|创建|新建|建立|建).{0,20}(表|索引|视图|table|index|view)", compact):
                action, risk = "create", "high"
            elif re.search(r"(?:字段|列|column).{0,40}(?:增加|新增|添加|加|删除|修改|重命名|add|drop|alter|rename)", compact) \
                    or re.search(r"(?:增加|新增|添加|加|删除|修改|重命名|add|alter|rename).{0,40}(?:字段|列|column)", compact):
                action, risk = "alter", "high"
            elif re.search(r"(删除|删掉|移除|\bdelete\b|\bremove\b)", compact):
                action, risk = "delete", "high"
            elif re.search(
                r"(新增|插入|添加一条|添加数据|添加记录|写入|录入|填入|\binsert\b|\badd\b)",
                compact,
            ):
                action = "insert"
            elif re.search(r"(更新|修改|改成|改为|设为|设置成|设置为|\bupdate\b)", compact):
                action = "update"

            # 引导录入只负责打开 schema 约束的表单，不允许模型生成或执行 SQL。
            if result.interaction == "guided_insert":
                action, risk = "insert", "medium"

            if action == "create" and not targets:
                new_name = self._new_object_name(question)
                if new_name:
                    targets = [new_name]

        if action == "select" and not targets and len(self.schema.tables) == 1 \
                and self._GENERIC_SINGLE_TABLE_QUERY_RE.search(question):
            targets = [next(iter(self.schema.tables))]

        return DatabaseOperationPlan(
            action=action,
            mode=mode,
            intent=result.intent,
            target_tables=targets,
            risk=risk,
            requires_confirmation=(mode == "write"),
            status="planned",
            confidence=result.confidence,
            reasoning=reasoning,
        )

    def clarification_for(self, question: str, plan: DatabaseOperationPlan) -> Optional[dict]:
        """确定性歧义门禁：只拦截会导致猜表、猜字段、猜值或猜范围的请求。"""
        missing = ""
        prompt = ""
        candidates: List[dict] = []
        existing_targets = [name for name in plan.target_tables if name in self.schema.tables]

        table_required_actions = {"insert", "update", "delete", "alter", "drop", "mutate"}
        if plan.action in table_required_actions and len(existing_targets) != 1:
            missing = "target_table"
            prompt = "这次操作只能明确针对一张表，请选择目标表。"
        elif plan.action == "mutate":
            missing = "operation_type"
            prompt = "要新增、修改还是删除数据？请明确操作类型。"
        elif plan.action == "create" and not plan.target_tables:
            missing = "object_name"
            prompt = "要创建的表、索引或视图叫什么名字？"
        elif plan.action == "create" and not re.search(
            r"\([^)]{3,}\)|(?:字段|列).{0,40}(?:类型|integer|int|text|real|blob|varchar|date|bool)",
            question,
            re.IGNORECASE,
        ):
            missing = "object_definition"
            prompt = "请提供要创建对象的字段定义，例如：id INTEGER 主键、name TEXT。"
        elif plan.action == "alter" and not re.search(
            r"(?:字段|列|column).{0,18}[`\"]?[\w$\u4e00-\u9fff]+[`\"]?.{0,18}"
            r"(?:integer|int|text|real|blob|varchar|date|bool|重命名为|改名为|删除)|"
            r"[`\"]?[\w$\u4e00-\u9fff]+[`\"]?\s+(?:integer|int|text|real|blob|varchar|date|bool)\s*"
            r"(?:字段|列|column)|"
            r"(?:删除|删掉|移除|drop).{0,24}[`\"]?[\w$\u4e00-\u9fff]+[`\"]?\s*(?:字段|列|column)|"
            r"(?:add|drop|rename)\s+(?:column\s+)?[`\"]?[\w$]+[`\"]?",
            question,
            re.IGNORECASE,
        ):
            missing = "schema_change_definition"
            prompt = "请明确字段名称、类型以及增加、删除或重命名操作。"
        elif plan.action == "select" and not plan.target_tables \
                and len(self.schema.tables) > 1 and self._GENERIC_SINGLE_TABLE_QUERY_RE.search(question):
            missing = "target_table"
            prompt = "要统计或查看哪张表？"
        elif plan.action in {"select", "analyze"} and len(existing_targets) > 1 \
                and not QueryBranchPlanner.independent_tables(question, existing_targets) \
                and not SchemaRelationAnalyzer(self.schema).analyze(existing_targets, question)["connected"]:
            missing = "table_relationship"
            prompt = (
                "这些表之间没有已声明的外键路径。请提供明确等值关联，"
                "例如 orders.customer_id = customers.id。"
            )
        elif plan.action in {"select", "analyze"} and self._DERIVED_METRIC_RE.search(question) \
                and not self._has_defined_metric(question) and not self._has_metric_definition(question):
            missing = "metric_definition"
            prompt = "这个业务指标尚未定义。请说明计算口径，例如分子、分母和过滤条件。"
        if not missing and plan.action in {"select", "analyze"}:
            drill = self.semantic_catalog.dimension_drill_request(question)
            if drill is not None and drill.get("status") != "resolved":
                missing = "dimension_level"
                if drill.get("status") == "needs_source":
                    prompt = "请明确要从哪个已配置业务维度开始下钻。"
                elif drill.get("status") == "invalid_target":
                    prompt = "目标维度必须是同一张表、同一层级路径中更深的层级。"
                else:
                    prompt = "请明确要下钻到哪个更深的业务维度。"
        if not missing and plan.action in {"select", "analyze"} and self._FIELD_AGGREGATE_RE.search(question) \
                and not self._has_defined_metric(question):
            numeric_candidates = self._candidate_columns(existing_targets, self._is_numeric_column)
            if numeric_candidates and not self._mentioned_candidates(question, numeric_candidates):
                missing = "aggregation_field"
                prompt = "要对哪个数值字段进行聚合？"
        if not missing and plan.action in {"select", "analyze"} \
                and self._BUSINESS_CALENDAR_RE.search(question) \
                and not self._has_defined_business_calendar(question) \
                and not self._has_business_calendar_definition(question):
            missing = "business_calendar"
            prompt = "请在语义层选择或新增业务日历，或说明一次性的财年起始日与工作日规则。"
        if not missing and plan.action in {"select", "analyze"} and self._TIME_SIGNAL_RE.search(question):
            time_candidates = self._candidate_columns(existing_targets, self._is_time_column)
            mentioned_time = self._mentioned_candidates(question, time_candidates)
            if len(time_candidates) > 1 and not mentioned_time:
                missing = "time_field"
                prompt = "这次分析应使用哪个时间字段？"
            elif time_candidates and self._VAGUE_TIME_RE.search(question) \
                    and not self._has_explicit_time_range(question):
                missing = "time_range"
                prompt = "请把时间范围说具体，例如最近 30 天或 2026-08-01 到 2026-08-31。"
            elif time_candidates and self._TIME_GRAIN_ANALYSIS_RE.search(question) \
                    and not self._has_explicit_time_grain(question) \
                    and not self._configured_time_grain(question, time_candidates, mentioned_time):
                missing = "time_grain"
                prompt = "这次趋势分析应按日、周、月、季度还是年聚合？"
        if not missing and plan.action == "update":
            target_columns = self._update_target_columns(question, existing_targets)
            if not target_columns:
                missing = "target_field"
                prompt = "要修改哪个字段？"
            elif not self._has_new_value(question):
                missing = "new_value"
                prompt = "这个字段要修改成什么值？"
            elif not self._has_filter_condition(question, existing_targets, plan.action):
                missing = "filter_condition"
                prompt = "哪些记录需要修改？请给出主键或明确筛选条件。"
        elif not missing and plan.action == "delete" \
                and not self._has_filter_condition(question, existing_targets, plan.action):
            missing = "filter_condition"
            prompt = "哪些记录需要删除？请给出主键或明确筛选条件。"
        elif not missing and plan.action == "insert":
            mentioned = self._target_columns(question, existing_targets)
            if not mentioned or not re.search(r"(=|为|是|值|内容|新增内容\s*[:：]|\bvalues?\b)", question, re.I):
                missing = "record_values"
                prompt = "请提供新增记录的字段和值。"

        if not missing:
            return None

        if missing == "target_table":
            table_names = existing_targets if len(existing_targets) > 1 else list(self.schema.tables)
            candidates = [
                {
                    "label": name,
                    "prompt": f"{question.rstrip('？?。；;')}；目标表：{name}",
                }
                for name in table_names[:8]
            ]
        elif missing == "target_field" and existing_targets:
            table = self.schema.tables[existing_targets[0]]
            candidates = [
                {
                    "label": column.name,
                    "prompt": f"{question.rstrip('？?。；;')}；目标字段：{column.name}",
                }
                for column in table.columns if not column.pk
            ][:8]
        elif missing == "aggregation_field":
            candidates = [
                {
                    "label": f"{table_name}.{column.name}",
                    "prompt": f"{question.rstrip('？?。；;')}；聚合字段：{table_name}.{column.name}",
                }
                for table_name, column in self._candidate_columns(existing_targets, self._is_numeric_column)
            ][:8]
        elif missing == "time_field":
            candidates = [
                {
                    "label": f"{table_name}.{column.name}",
                    "prompt": f"{question.rstrip('？?。；;')}；时间字段：{table_name}.{column.name}",
                }
                for table_name, column in self._candidate_columns(existing_targets, self._is_time_column)
            ][:8]
        elif missing == "time_range":
            candidates = [
                {
                    "label": label,
                    "prompt": f"{question.rstrip('？?。；;')}；时间范围：{value}",
                }
                for label, value in (
                    ("最近 7 天", "最近 7 天"),
                    ("最近 30 天", "最近 30 天"),
                    ("最近 90 天", "最近 90 天"),
                )
            ]
        elif missing == "time_grain":
            candidates = [
                {
                    "label": label,
                    "prompt": f"{question.rstrip('？?。；;')}；时间粒度：{value}",
                }
                for label, value in (
                    ("按日", "日"),
                    ("按周", "周"),
                    ("按月", "月"),
                    ("按季度", "季度"),
                    ("按年", "年"),
                )
            ]
        elif missing == "dimension_level":
            drill = self.semantic_catalog.dimension_drill_request(question) or {}
            candidates = [
                {
                    "label": str(item.get("term") or ""),
                    "prompt": (
                        f"{question.rstrip('？?。；;')}；"
                        f"维度层级：{item.get('term')}"
                    ),
                }
                for item in drill.get("candidates") or []
                if item.get("term")
            ][:8]
        elif missing == "business_calendar":
            calendars = [
                item for item in self.semantic_catalog.entries
                if item.get("kind") == "business_calendar"
                and (not existing_targets or item.get("table") in existing_targets)
            ]
            candidates = [
                {
                    "label": str(item.get("term") or "业务日历"),
                    "prompt": f"{question.rstrip('？?。；;')}；业务日历：{item.get('term')}",
                }
                for item in calendars
            ][:8]

        missing_labels = {
            "target_table": "目标表",
            "object_name": "对象名称",
            "target_field": "目标字段",
            "new_value": "修改后的值",
            "filter_condition": "筛选条件",
            "record_values": "新增记录的字段和值",
            "operation_type": "操作类型",
            "object_definition": "字段定义",
            "schema_change_definition": "结构变更定义",
            "table_relationship": "表关联条件",
            "metric_definition": "指标口径",
            "aggregation_field": "聚合字段",
            "time_field": "时间字段",
            "time_range": "时间范围",
            "time_grain": "时间粒度",
            "dimension_level": "维度层级",
            "business_calendar": "业务日历口径",
        }
        return {
            "missing": missing,
            "missing_label": missing_labels[missing],
            "question": prompt,
            "candidates": candidates,
            "input_hint": "可以直接回复补充内容，也可以重新输入一条完整指令。",
            "original_question": question,
        }

    @staticmethod
    def clarification_answer(plan: DatabaseOperationPlan, clarification: dict) -> DBAnswer:
        plan.status = "needs_clarification"
        label = clarification["missing_label"]
        narrative = f"为了避免猜测或误操作，我还不能执行。请先补充{label}。"
        return DBAnswer(
            kind="clarification",
            narrative=narrative,
            operation=plan.as_dict(),
            clarification=clarification,
            steps=[{
                "tool": "ambiguity_gate",
                "status": "needs_clarification",
                "missing": clarification["missing"],
            }],
        )

    @staticmethod
    def enrich(plan: DatabaseOperationPlan, answer: DBAnswer) -> DatabaseOperationPlan:
        """使用实际生成结果修正计划，但不改变写操作必须确认的安全约束。"""
        plan.sql = answer.sql
        write = answer.write or {}
        if plan.mode == "write":
            actual_action = str(write.get("kind") or "").strip().lower()
            if actual_action in {"insert", "update", "delete", "create", "alter", "drop"}:
                plan.action = actual_action
            actual_table = str(write.get("table") or "").strip()
            if actual_table:
                plan.target_tables = [actual_table]
            if plan.action in {"delete", "create", "alter", "drop"} or bool(write.get("dangerous")):
                plan.risk = "high"
            plan.requires_confirmation = True
        if answer.kind == "write_form":
            plan.status = "needs_clarification"
        elif answer.kind == "write_pending":
            plan.status = "awaiting_confirmation"
        elif answer.kind in {"query", "retrieve", "compose", "schema", "write_result"}:
            plan.status = "executed"
        elif answer.kind == "error":
            plan.status = "failed"
        return plan


class SchemaOperationExecutor:
    """直接执行结构查看计划，稳定、快速且不消耗模型调用。"""

    def __init__(self, schema: SchemaSnapshot):
        self.schema = schema

    def answer(self, plan: DatabaseOperationPlan) -> DBAnswer:
        if plan.action == "inspect_relations" and len(plan.target_tables) >= 2:
            wanted_set = {name for name in plan.target_tables if name in self.schema.tables}
            rows = []
            for table in self.schema.tables.values():
                if table.name not in wanted_set:
                    continue
                for col in table.columns:
                    if col.fk_table and col.fk_table in wanted_set:
                        rows.append([
                            f"{table.name}.{col.name}",
                            f"{col.fk_table}.{col.fk_column or ''}".rstrip("."),
                        ])
            if rows:
                edges = "；".join(f"{r[0]} → {r[1]}" for r in rows)
                narrative = f"这些表之间已声明的外键关系：{edges}。"
            else:
                narrative = (
                    "这些表之间没有已声明的外键。"
                    "如需跨表查询，请显式提供等值关联条件，系统不会猜测表关系。"
                )
            return DBAnswer(
                kind="schema",
                narrative=narrative,
                columns=["外键来源", "外键指向"],
                rows=rows,
            )
        if plan.action == "inspect_table" and plan.target_tables:
            table_name = plan.target_tables[0]
            table = self.schema.tables.get(table_name)
            if table is None:
                return DBAnswer(kind="error", narrative="没有找到指定表。", error="table not found")
            rows = []
            for col in table.columns:
                foreign_key = (
                    f"{col.fk_table}.{col.fk_column}"
                    if col.fk_table and col.fk_column else ""
                )
                rows.append([
                    col.name,
                    col.type or "未声明",
                    "是" if col.pk else "否",
                    "是" if col.nullable else "否",
                    foreign_key,
                ])
            return DBAnswer(
                kind="schema",
                narrative=f"表 {table_name} 共有 {table.row_count} 行、{len(table.columns)} 个字段。",
                columns=["字段", "类型", "主键", "可空", "外键"],
                rows=rows,
            )

        rows = [
            [table.name, table.row_count, len(table.columns)]
            for table in self.schema.tables.values()
        ]
        return DBAnswer(
            kind="schema",
            narrative=f"当前数据库共有 {len(rows)} 张表。",
            columns=["表名", "行数", "字段数"],
            rows=rows,
        )


# ---------------------------------------------------------------------------
# BasicConversationRouter —— 高置信本地基础沟通，与数据库执行隔离
# ---------------------------------------------------------------------------

class BasicConversationRouter:
    """处理问候、感谢、身份、能力、用法和告别等基础沟通。

    路由只接受整句高置信社交表达，不调用模型、不读取数据行、不生成 SQL，
    也不会因句首出现“你好/谢谢”而拦截后续数据库指令。
    """

    _GREETING_RE = re.compile(
        r"(?:你好|您好|嗨|哈[喽啰罗]|早上好|早安|下午好|晚上好|"
        r"在吗|你在吗|有人吗|hello|hi|hey)(?:你)?(?:啊|呀|哈|呢)?",
        re.IGNORECASE,
    )
    _THANKS_RE = re.compile(
        r"(?:谢谢|多谢|感谢|辛苦(?:你)?了|thanks?|thank\s+you|thx)(?:你|啦|了|啊|呀)?",
        re.IGNORECASE,
    )
    _ACK_RE = re.compile(
        r"(?:好|好的|好吧|行|可以|明白(?:了)?|知道(?:了)?|收到|没问题)",
        re.IGNORECASE,
    )
    _FAREWELL_RE = re.compile(
        r"(?:再见|拜拜|回头见|下次见|晚安|bye|goodbye|see\s+you)",
        re.IGNORECASE,
    )
    _IDENTITY_RE = re.compile(
        r"(?:你是谁|你是什么|你叫什么|介绍一下你自己|自我介绍|who\s+are\s+you)",
        re.IGNORECASE,
    )
    _CAPABILITY_RE = re.compile(
        r"(?:你能做什么|你会什么|你可以做什么|有什么功能|"
        r"怎么用(?:你|这个工具|这个系统)?|如何使用(?:你|这个工具|这个系统)?|"
        r"使用帮助|帮助|help|what\s+can\s+you\s+do)",
        re.IGNORECASE,
    )
    _WELLBEING_RE = re.compile(
        r"(?:你好吗|你还好吗|最近怎么样|今天怎么样|how\s+are\s+you)",
        re.IGNORECASE,
    )
    _CHAT_RE = re.compile(
        r"(?:聊聊天|陪我聊聊|可以聊天吗|能聊聊吗|想和你聊聊)",
        re.IGNORECASE,
    )
    _MOOD_RE = re.compile(
        r"我(?:今天)?(?:有点|很|太)?(?P<mood>累|困|烦|难过|不开心|开心|高兴|焦虑|紧张)(?:了|啊|呀)?",
        re.IGNORECASE,
    )
    _APOLOGY_RE = re.compile(r"(?:对不起|抱歉|sorry)", re.IGNORECASE)
    _JOKE_RE = re.compile(r"(?:讲个笑话|说个笑话|来个笑话)", re.IGNORECASE)
    _EDGE_PUNCT_RE = re.compile(r"^[\s，。！？、,!.?~～；;:：]+|[\s，。！？、,!.?~～；;:：]+$")

    @classmethod
    def _normalized(cls, question: str) -> str:
        text = re.sub(r"\s+", " ", str(question or "").strip())
        return cls._EDGE_PUNCT_RE.sub("", text)

    @staticmethod
    def _matches_pending_candidate(text: str, clarification: Optional[dict]) -> bool:
        if not clarification:
            return False
        folded = text.casefold()
        for candidate in clarification.get("candidates") or []:
            for key in ("label", "prompt"):
                value = str(candidate.get(key) or "").strip()
                if value and value.casefold() == folded:
                    return True
        return False

    @staticmethod
    def _pending_suffix(clarification: Optional[dict]) -> str:
        if not clarification:
            return ""
        label = str(clarification.get("missing_label") or "必要信息").strip()
        return f"刚才待补充的“{label}”仍然保留，准备好后直接回复即可。"

    def answer(
        self,
        question: str,
        *,
        clarification: Optional[dict] = None,
    ) -> Optional[DBAnswer]:
        text = self._normalized(question)
        if not text or len(text) > 80 or self._matches_pending_candidate(text, clarification):
            return None

        narrative = ""
        if self._GREETING_RE.fullmatch(text):
            narrative = "你好，我在。你可以直接问数据、查看表结构，或告诉我想完成的操作。"
        elif self._THANKS_RE.fullmatch(text):
            narrative = "不客气。你可以继续问数据，也可以换一个问题。"
        elif self._FAREWELL_RE.fullmatch(text):
            narrative = "再见。下次打开 DB-Agent 后，仍可以从当前数据库继续。"
        elif self._IDENTITY_RE.fullmatch(text):
            narrative = (
                "我是 DB-Agent，一个以自然语言驱动数据库操作的本地桌面助手。"
                "我会把查询、分析和受控写入分开处理，也能进行简单的日常沟通。"
            )
        elif self._CAPABILITY_RE.fullmatch(text):
            narrative = (
                "我可以：\n"
                "1. 查看数据库、表和字段结构；\n"
                "2. 用自然语言查询、筛选、统计和分析数据；\n"
                "3. 检索记录中的文本内容；\n"
                "4. 对写入先校验并生成变更预览，由你确认后才执行；\n"
                "5. 回应问候、感谢，并说明怎么使用。\n"
                "例如可以说：“有哪些表？”“统计 items 的记录数”或“录入一条数据”。"
            )
        elif self._WELLBEING_RE.fullmatch(text):
            narrative = "我状态正常，可以开始。你今天想查数据，还是先聊两句？"
        elif self._CHAT_RE.fullmatch(text):
            narrative = "当然可以。你想聊什么？想回到数据库工作时，直接告诉我要查或要改什么就行。"
        elif match := self._MOOD_RE.fullmatch(text):
            mood = match.group("mood")
            if mood in {"开心", "高兴"}:
                narrative = "听起来是件好事。如果你愿意，可以说说发生了什么。"
            else:
                narrative = "听起来你现在不太轻松。可以先说说是什么让你这样，我会认真听。"
        elif self._APOLOGY_RE.fullmatch(text):
            narrative = "没关系。我们可以继续，你也可以重新说一遍想做的事。"
        elif self._JOKE_RE.fullmatch(text):
            narrative = "数据库为什么不爱争论？因为它更喜欢用事实说话——而且还要有索引。"
        elif self._ACK_RE.fullmatch(text) and not clarification:
            narrative = "好的。你继续说，我来处理。"
        else:
            return None

        suffix = self._pending_suffix(clarification)
        if suffix:
            narrative = f"{narrative}\n{suffix}"
        return DBAnswer(kind="conversation", narrative=narrative)


# ---------------------------------------------------------------------------
# IntentRouter —— LLM 判断数据库意图 + 置信度
# ---------------------------------------------------------------------------

class IntentRouter:
    """判断问题意图：query(要算) / retrieve(要描述) / compose(组合推理) / write(写库)。"""

    _PROMPT = (
        "判断下面问题对 sqlite 数据库的意图，只输出一个 JSON 对象：\n"
        "{{\"intent\": \"query\" | \"retrieve\" | \"compose\" | \"write\", "
        "\"interaction\": \"auto\" | \"guided_insert\" | \"direct_write\", "
        "\"target_table\": \"真实表名或空字符串\", "
        "\"confidence\": 0.0~1.0, \"reasoning\": \"简短中文说明\"}}\n"
        "意图定义：\n"
        "- query：需要计算/统计/筛选/排序等具体数据操作（COUNT/AVG/JOIN/LIMIT 等）；"
        "列出或查找论文、文献、文档、资料等列表也属于 query（需要真实查询数据库）；"
        "引用上一轮结果中的具体条目（如“第一篇”“其中 2025 年那篇”）要求其字段详情，也属于 query\n"
        "- retrieve：需要描述性内容/查资料/了解知识（答案蕴含在记录文本中，且不要求列举清单）\n"
        "- compose：需要多步组合推理（先查数，再结合检索/规则得出结论）\n"
        "- write：用户明确要求修改/删除/新增数据或建表/改表结构（更新/插入/删除/创建表/删除表等）。"
        "典型表达：把…改成…、将…更新为…、删除…、新增一条…、创建…表、给…添加一列。"
        "只要用户想让数据库内容或结构发生变化，就是 write。\n"
        "交互方式：\n"
        "- guided_insert：用户想写入/录入/新增数据，但还没有给出完整字段和值。"
        "例如‘能不能写入’‘我想往库里加点东西’‘帮我录一条客户数据’"
        "‘向 orders 表新增一条’。此时产品应打开选表或录入表单；\n"
        "- direct_write：用户给出了具体字段和值，或要求更新、删除、创建、修改结构；\n"
        "- auto：所有非 write 意图。\n"
        "target_table 只能填写数据库结构里出现的一个真实物理表名；无法唯一确定时必须为空字符串。\n\n"
        "数据库结构：\n{schema}\n\n"
        "用户问题：{question}\n\n"
        "输出 JSON："
    )

    # 明确的高风险/完整写操作保留本地安全兜底；不完整录入请求交给模型决定交互方式。
    _WRITE_STRUCT_RE = re.compile(
        r"(把|将)[^，。；,;]{1,40}(改成|改为|设为|设置成|设置为|删除|删掉|更新|修改|移除|改一下|变更为|调成)"
    )
    _WRITE_START_RE = re.compile(
        r"^(请|帮我|麻烦|替我|能不能|可以|我要|我想)?"
        r"(删除|删掉|移除|新增|插入|添加|创建|更新|修改|设置|写入|录入|填入|改成|改为|建|建立)"
    )
    _GUIDED_WRITE_HINT_RE = re.compile(
        r"(?:写入|录入|填入|新增|插入|添加|加(?:一条|一行|点|个)?(?:数据|记录|东西)?|"
        r"\binsert\b|\badd\b)",
        re.IGNORECASE,
    )
    _GUIDED_WRITE_EXCLUDE_RE = re.compile(
        r"(?:删除|删掉|移除|更新|修改|改成|改为|设为|设置|创建|新建|建立|"
        r"字段|列|column|table|index|view|\bdelete\b|\bupdate\b|\bdrop\b|\balter\b)",
        re.IGNORECASE,
    )
    _GUIDED_WRITE_VALUE_RE = re.compile(
        r"=|[:：]\s*\S|[\"'“‘][^\"'”’]{1,240}[\"'”’]|"
        r"(?:字段|姓名|名称|城市|地址|状态|编号|日期|金额).{0,12}(?:为|是|改成|设为)",
        re.IGNORECASE,
    )
    _WRITE_SCHEMA_RE = re.compile(
        r"^(请|帮我|麻烦|替我|能不能|可以)?(给|为).{1,80}"
        r"(添加|增加|新增|加|删除|移除|修改|重命名).{0,40}(字段|列|column)",
        re.IGNORECASE,
    )
    _QUERY_MARK_RE = re.compile(
        r"(怎么|如何|怎样|查询|查找|统计|计算|列出|展示|显示|有多少|是什么|有哪些|帮我查|查一下|看看|介绍|了解|什么意思|什么情况)"
    )
    _COUNT_OR_LIST_RE = re.compile(
        r"(一共|总共|总计|总共有)?.{0,8}(多少条|多少行|记录数|行数)|"
        r"(前|最近)\s*\d+\s*(条|行).{0,8}(数据|记录)|"
        r"\b(count|row\s+count|first\s+\d+\s+rows?)\b",
        re.IGNORECASE,
    )
    _ANALYTIC_QUERY_RE = re.compile(
        r"(统计|计算|平均|均值|合计|总额|总和|求和|转化率|留存率|复购率|客单价|增长率|"
        r"利润率|毛利率|达成率|完成率|成功率|失败率|趋势|同比|环比|"
        r"\b(?:sum|avg|average|max|min|trend)\b)",
        re.IGNORECASE,
    )
    _COMPOSE_FLOW_RE = re.compile(
        r"(分别|各自|逐个|分开|各算各的).{0,40}"
        r"(数量|多少|统计|计算|平均|合计|总额|对比|比较)|"
        r"(如果|若|如有).{0,32}(再|则|就).{0,20}"
        r"(检索|查找|查看|说明|分析|总结|内容|详情|原因|记录)|"
        r"\b(?:respectively|separately)\b|\bif\b.{0,40}\bthen\b",
        re.IGNORECASE,
    )
    _SOCIAL_PREFACE_RE = re.compile(
        r"^(?:你好|您好|嗨|哈[喽啰罗]|谢谢|多谢|感谢|hello|hi|hey|thanks?|thank\s+you)"
        r"(?:你|啦|了|啊|呀)?[\s，。！？、,!.?;；:：]+",
        re.IGNORECASE,
    )

    def __init__(self, llm_cfg: str = "default"):
        self.llm_cfg = llm_cfg

    @classmethod
    def _looks_like_guided_insert(cls, question: str) -> bool:
        text = str(question or "").strip()
        return bool(
            cls._GUIDED_WRITE_HINT_RE.search(text)
            and not cls._GUIDED_WRITE_EXCLUDE_RE.search(text)
            and not cls._GUIDED_WRITE_VALUE_RE.search(text)
        )

    def classify(self, question: str, schema_compact: str, history: Optional[list] = None) -> IntentResult:
        rule_question = self._SOCIAL_PREFACE_RE.sub("", question.strip(), count=1)
        q2 = re.sub(r"[，。？！!?？\s]", "", rule_question)
        guided_insert_hint = self._looks_like_guided_insert(rule_question)
        if not guided_insert_hint and not self._QUERY_MARK_RE.search(q2) and (
            self._WRITE_STRUCT_RE.search(q2) or self._WRITE_START_RE.match(q2)
            or self._WRITE_SCHEMA_RE.match(q2)
        ):
            return IntentResult(
                intent="write",
                confidence=0.95,
                reasoning="本地安全规则命中明确写操作动词，防止写请求降级为只读",
                interaction="direct_write",
                source="safety_guard",
            )
        if self._COMPOSE_FLOW_RE.search(rule_question):
            return IntentResult(intent="compose", confidence=0.95, reasoning="规则命中独立多查询或条件分析表达，进入只读操作图", source="deterministic")
        if self._COUNT_OR_LIST_RE.search(rule_question):
            return IntentResult(intent="query", confidence=0.95, reasoning="规则命中计数或取前若干行表达，判定为查询意图", source="deterministic")
        if self._ANALYTIC_QUERY_RE.search(rule_question):
            return IntentResult(intent="query", confidence=0.95, reasoning="规则命中聚合、指标或时间分析表达，判定为查询意图", source="deterministic")
        prompt = self._PROMPT.format(schema=schema_compact, question=question)
        try:
            obj = _llm_ask_json(prompt, self.llm_cfg, history=history)
            intent = (obj.get("intent") or "").strip().lower()
            if intent not in ("query", "retrieve", "compose", "write"):
                raise DBAgentError(f"意图非法: {intent!r}")
            interaction = str(obj.get("interaction") or "").strip().lower()
            if intent != "write":
                interaction = "auto"
            elif interaction not in {"guided_insert", "direct_write"}:
                interaction = "guided_insert" if guided_insert_hint else "direct_write"
            target_table = str(obj.get("target_table") or "").strip() if intent == "write" else ""
            return IntentResult(
                intent=intent,
                confidence=float(obj.get("confidence") or 0.0),
                reasoning=str(obj.get("reasoning") or ""),
                interaction=interaction,
                target_table=target_table,
                source="model",
            )
        except (DBAgentError, ValueError, TypeError) as e:
            if guided_insert_hint:
                return IntentResult(
                    intent="write",
                    confidence=0.55,
                    reasoning=f"模型意图分类不可用，本地安全兜底保留引导录入入口: {e}",
                    interaction="guided_insert",
                    source="safety_fallback",
                )
            # 解析失败走保守降级：retrieve（不产生 SQL，最安全）
            return IntentResult(intent="retrieve", confidence=0.3, reasoning=f"意图分类失败，降级为检索: {e}", source="safety_fallback")


# ---------------------------------------------------------------------------
# OperationGraph —— 自研只读多步骤操作图
# ---------------------------------------------------------------------------

class QueryBranchPlanner:
    """识别可安全拆开的独立查数分支；不推断 JOIN，也不生成任意节点。"""

    MAX_INDEPENDENT_BRANCHES = 6

    _INDEPENDENT_RE = re.compile(
        r"(分别|各自|逐个|分开|各算各的|分别统计|分别计算|各自统计|各自计算|"
        r"\b(?:respectively|separately|each)\b)",
        re.IGNORECASE,
    )
    _CONDITIONAL_RETRIEVE_RE = re.compile(
        r"(如果|若|如有|有.{0,6}(?:数据|记录|结果)).{0,20}"
        r"(再|则|就).{0,12}(检索|查找|查看|说明|分析|总结|内容|详情|原因|记录)|"
        r"\bif\b.{0,30}\b(?:then|retrieve|explain|summari[sz]e)\b",
        re.IGNORECASE,
    )
    _RELATION_SIGNAL_RE = re.compile(
        r"(关联|联结|连接|匹配|对应关系|关联条件|\bjoin\b)",
        re.IGNORECASE,
    )

    @classmethod
    def independent_tables(cls, question: str, tables: List[str]) -> List[str]:
        targets = list(dict.fromkeys(tables))
        if len(targets) < 2 or not cls._INDEPENDENT_RE.search(question or "") \
                or cls._RELATION_SIGNAL_RE.search(question or ""):
            return []
        return targets

    @classmethod
    def conditional_retrieval(cls, question: str) -> bool:
        return bool(cls._CONDITIONAL_RETRIEVE_RE.search(question or ""))

    @staticmethod
    def branch_question(question: str, table: str) -> str:
        return (
            f"{question.rstrip()}\n"
            f"查询分支约束：只回答表 {table} 的独立结果，不关联或读取其他表。"
        )

class OperationGraphPlanner:
    """按问题特征确定性选择只读节点；模型不能发明工具或依赖。"""

    _QUANTITATIVE_RE = re.compile(
        r"(数量|多少|统计|计算|总计|合计|平均|均值|最大|最小|排名|排行|趋势|占比|比例|"
        r"增长|下降|变化|对比|比较|差异|金额|总额|sum|count|avg|max|min|rank|trend)",
        re.IGNORECASE,
    )
    _CONTEXTUAL_RE = re.compile(
        r"(记录内容|内容|记录|详情|描述|说明|原因|依据|文本|备注|评价|反馈|知识|背景|"
        r"为什么|怎么回事|结合|context|detail|reason|evidence|description)",
        re.IGNORECASE,
    )

    def __init__(
        self,
        schema: Optional[SchemaSnapshot] = None,
        semantic_catalog: Optional[SemanticCatalog] = None,
    ):
        self.schema = schema
        self.semantic_catalog = semantic_catalog

    def _target_tables(self, question: str) -> List[str]:
        if self.schema is None:
            return []
        folded = question.casefold()
        targets: List[str] = []
        for name in self.schema.tables:
            key = name.casefold()
            matched = (
                bool(re.search(rf"(?<![a-z0-9_]){re.escape(key)}(?![a-z0-9_])", folded))
                if re.fullmatch(r"[a-z0-9_]+", key)
                else key in folded
            )
            if matched:
                targets.append(name)
        if self.semantic_catalog is not None:
            for match in self.semantic_catalog.resolve(question).matches:
                name = str(match.get("table") or "")
                if name in self.schema.tables and name not in targets:
                    targets.append(name)
        return targets

    def plan_compose(
        self,
        question: str,
        target_tables: Optional[List[str]] = None,
    ) -> OperationGraph:
        objective = question.strip()
        targets = list(dict.fromkeys(target_tables or self._target_tables(objective)))
        needs_query = bool(self._QUANTITATIVE_RE.search(objective))
        needs_retrieve = bool(self._CONTEXTUAL_RE.search(objective))
        independent_targets = QueryBranchPlanner.independent_tables(objective, targets)
        conditional_retrieval = bool(
            needs_query and needs_retrieve
            and QueryBranchPlanner.conditional_retrieval(objective)
        )
        if not needs_query and not needs_retrieve:
            needs_query = True
            needs_retrieve = True

        nodes: List[OperationGraphNode] = []
        relation_node_id = ""
        if needs_query and len(targets) > 1 and not independent_targets:
            relation_node_id = "inspect-relations"
            nodes.append(OperationGraphNode(
                node_id=relation_node_id,
                tool="inspect_relations",
                input=objective,
                parameters={"tables": targets},
                failure_policy="stop",
            ))

        result_nodes: List[str] = []
        if needs_query:
            query_targets = independent_targets or [None]
            for index, branch_table in enumerate(query_targets, 1):
                node_id = "query-data" if branch_table is None else f"query-{index}"
                branch_tables = targets if branch_table is None else [branch_table]
                branch_input = (
                    objective if branch_table is None
                    else QueryBranchPlanner.branch_question(objective, branch_table)
                )
                result_nodes.append(node_id)
                nodes.append(OperationGraphNode(
                    node_id=node_id,
                    tool="query",
                    input=branch_input,
                    parameters={
                        "question": branch_input,
                        "tables": branch_tables,
                        "allowed_tables": branch_tables if branch_table is not None else [],
                        "branch_label": branch_table or "",
                    },
                    depends_on=[relation_node_id] if relation_node_id else [],
                    failure_policy="continue",
                ))
        if needs_retrieve:
            result_nodes.append("retrieve-context")
            retrieve_dependencies = list(result_nodes[:-1]) if conditional_retrieval else []
            nodes.append(OperationGraphNode(
                node_id="retrieve-context",
                tool="retrieve",
                input=objective,
                parameters={
                    "question": objective,
                    "tables": targets,
                    "condition": "has_data" if conditional_retrieval else "",
                },
                depends_on=retrieve_dependencies,
                failure_policy="continue",
            ))
        nodes.append(OperationGraphNode(
            node_id="synthesize-answer",
            tool="synthesize",
            input=objective,
            parameters={"dependencies": result_nodes},
            depends_on=result_nodes,
            failure_policy="stop",
        ))
        return OperationGraph(
            objective=objective,
            nodes=nodes,
            strategy=(
                "deterministic-multi-query-conditional"
                if independent_targets and conditional_retrieval
                else "deterministic-multi-query" if independent_targets
                else "deterministic-conditional" if conditional_retrieval
                else "deterministic"
            ),
            target_tables=targets,
        )


class OperationGraphValidator:
    """校验节点白名单、依赖完整性与无环性，返回稳定拓扑序。"""

    ALLOWED_TOOLS = frozenset(OPERATION_GRAPH_CONTRACTS)
    ALLOWED_FAILURE_POLICIES = frozenset({"continue", "stop"})
    ALLOWED_STRATEGIES = frozenset({
        "deterministic",
        "deterministic-multi-query",
        "deterministic-conditional",
        "deterministic-multi-query-conditional",
    })
    ALLOWED_CONDITIONS = frozenset({"", "has_data"})
    MULTI_QUERY_STRATEGIES = frozenset({
        "deterministic-multi-query",
        "deterministic-multi-query-conditional",
    })
    MAX_NODES = 8

    def validate(self, graph: OperationGraph) -> List[OperationGraphNode]:
        if not graph.objective.strip():
            raise OrchestratorError("操作图缺少目标")
        if not graph.nodes:
            raise OrchestratorError("操作图不包含节点")
        if graph.strategy not in self.ALLOWED_STRATEGIES:
            raise OrchestratorError(f"操作图策略非法: {graph.strategy}")
        if len(graph.nodes) > self.MAX_NODES:
            raise OrchestratorError(f"操作图节点超过上限 {self.MAX_NODES}")
        if any(not isinstance(name, str) or not name for name in graph.target_tables) \
                or len(graph.target_tables) != len(set(graph.target_tables)):
            raise OrchestratorError("操作图目标表非法或重复")

        node_by_id: Dict[str, OperationGraphNode] = {}
        for node in graph.nodes:
            if not re.fullmatch(r"[a-z][a-z0-9-]{0,47}", node.node_id):
                raise OrchestratorError(f"操作图节点 ID 非法: {node.node_id!r}")
            if node.node_id in node_by_id:
                raise OrchestratorError(f"操作图节点 ID 重复: {node.node_id}")
            if node.tool not in self.ALLOWED_TOOLS:
                raise OrchestratorError(f"操作图工具不在只读白名单: {node.tool}")
            if node.failure_policy not in self.ALLOWED_FAILURE_POLICIES:
                raise OrchestratorError(f"操作图失败策略非法: {node.failure_policy}")
            condition = str(node.parameters.get("condition") or "")
            if condition not in self.ALLOWED_CONDITIONS:
                raise OrchestratorError(f"操作图分支条件非法: {node.node_id}")
            if condition and node.tool != "retrieve":
                raise OrchestratorError(f"操作图分支条件只能用于检索节点: {node.node_id}")
            if condition and not node.depends_on:
                raise OrchestratorError(f"条件分支缺少上游查询: {node.node_id}")
            allowed_tables = node.parameters.get("allowed_tables") or []
            if allowed_tables:
                if node.tool != "query" or not isinstance(allowed_tables, list) \
                        or any(not isinstance(name, str) or not name for name in allowed_tables):
                    raise OrchestratorError(f"操作图查询表范围非法: {node.node_id}")
                if len(allowed_tables) != 1:
                    raise OrchestratorError(f"独立查询分支必须且只能绑定一张表: {node.node_id}")
                if graph.target_tables and not set(allowed_tables).issubset(graph.target_tables):
                    raise OrchestratorError(f"操作图查询表范围越界: {node.node_id}")
            expected_contracts = OPERATION_GRAPH_CONTRACTS[node.tool]
            for direction in ("input", "output"):
                actual = getattr(node, f"{direction}_contract")
                expected = expected_contracts[direction]
                if actual.get("type") != expected["type"]:
                    raise OrchestratorError(
                        f"操作图节点契约非法: {node.node_id} {direction} 应为 {expected['type']}"
                    )
                if set(actual.get("required") or []) != set(expected["required"]):
                    raise OrchestratorError(f"操作图节点契约字段非法: {node.node_id} {direction}")
            if len(node.depends_on) != len(set(node.depends_on)):
                raise OrchestratorError(f"操作图节点存在重复依赖: {node.node_id}")
            node_by_id[node.node_id] = node

        synthesis_nodes = [node for node in graph.nodes if node.tool == "synthesize"]
        if len(synthesis_nodes) != 1 or not synthesis_nodes[0].depends_on:
            raise OrchestratorError("操作图必须有且只有一个带依赖的综合节点")

        query_nodes = [node for node in graph.nodes if node.tool == "query"]
        scoped_queries = [node for node in query_nodes if node.parameters.get("allowed_tables")]
        is_multi_query = graph.strategy in self.MULTI_QUERY_STRATEGIES
        if is_multi_query:
            branch_count = len(scoped_queries)
            if branch_count != len(query_nodes):
                raise OrchestratorError("多分支操作图中的查询必须全部限定为单表")
            if not 2 <= branch_count <= QueryBranchPlanner.MAX_INDEPENDENT_BRANCHES:
                raise OrchestratorError(
                    "独立查询分支数量必须在 2 到 "
                    f"{QueryBranchPlanner.MAX_INDEPENDENT_BRANCHES} 之间"
                )
            branch_tables = [node.parameters["allowed_tables"][0] for node in scoped_queries]
            if len(branch_tables) != len(set(branch_tables)):
                raise OrchestratorError("独立查询分支存在重复目标表")
            if branch_tables != graph.target_tables:
                raise OrchestratorError("独立查询分支必须按顺序完整覆盖目标表")
            if any(node.depends_on for node in scoped_queries):
                raise OrchestratorError("独立查询分支不能依赖关系推断或其他节点")
            if any(str(node.parameters.get("branch_label") or "") != table
                   for node, table in zip(scoped_queries, branch_tables)):
                raise OrchestratorError("独立查询分支标签必须与绑定表一致")
            if any(node.tool == "inspect_relations" for node in graph.nodes):
                raise OrchestratorError("独立查询操作图不能包含关系推断节点")
            synthesis_dependencies = set(synthesis_nodes[0].depends_on)
            if any(node.node_id not in synthesis_dependencies for node in scoped_queries):
                raise OrchestratorError("综合节点必须覆盖全部独立查询分支")
        elif scoped_queries:
            raise OrchestratorError("单表范围查询只能用于显式独立多分支策略")

        indegree = {node_id: 0 for node_id in node_by_id}
        dependents: Dict[str, List[str]] = {node_id: [] for node_id in node_by_id}
        for node in graph.nodes:
            for dependency in node.depends_on:
                if dependency == node.node_id:
                    raise OrchestratorError(f"操作图节点不能依赖自身: {node.node_id}")
                if dependency not in node_by_id:
                    raise OrchestratorError(f"操作图依赖不存在: {dependency}")
                indegree[node.node_id] += 1
                dependents[dependency].append(node.node_id)
            if node.parameters.get("condition") == "has_data" and any(
                node_by_id[dependency].tool != "query" for dependency in node.depends_on
            ):
                raise OrchestratorError(f"条件分支只能依赖查询节点: {node.node_id}")

        queue = [node.node_id for node in graph.nodes if indegree[node.node_id] == 0]
        ordered_ids: List[str] = []
        while queue:
            node_id = queue.pop(0)
            ordered_ids.append(node_id)
            for dependent in dependents[node_id]:
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    queue.append(dependent)
        if len(ordered_ids) != len(graph.nodes):
            raise OrchestratorError("操作图存在循环依赖")

        synthesis_id = synthesis_nodes[0].node_id
        if dependents[synthesis_id]:
            raise OrchestratorError("综合节点必须是操作图终点")
        for node in graph.nodes:
            missing_input = [
                name for name in node.input_contract["required"]
                if name == "question" and not node.input.strip()
                or name == "dependencies" and not node.depends_on
                or name not in {"question", "dependencies"} and name not in node.parameters
            ]
            if missing_input:
                raise OrchestratorError(
                    f"操作图节点输入缺少契约字段: {node.node_id} {', '.join(missing_input)}"
                )
        return [node_by_id[node_id] for node_id in ordered_ids]


class OperationGraphExecutor:
    """按拓扑序执行已验证图，并记录每个节点的状态、摘要和错误。"""

    MAX_INTERMEDIATE_ROWS = 100
    MAX_INTERMEDIATE_EVIDENCE = 20
    MAX_SYNTHESIS_ROWS_PER_QUERY = 20
    MAX_SYNTHESIS_CONTEXT_CHARS = 12000

    _COMPOSE_PROMPT = (
        "你是数据库问答助手。下面是针对同一问题的可用中间结果：\n"
        "【查数结果】\n{sql_part}\n\n"
        "【检索结果】\n{rag_part}\n\n"
        "请综合可用结果，用中文回答用户问题（引用数据时要准确，冲突时以查数结果为准并说明）。\n"
        "只输出 JSON：{{\"answer_zh\": \"...\"}}\n\n"
        "用户问题：{question}"
    )

    def __init__(
        self,
        nl2sql: NL2SQLExecutor,
        rag: RagRetriever,
        schema: Optional[SchemaSnapshot] = None,
        validator: Optional[OperationGraphValidator] = None,
        llm_cfg: str = "default",
    ):
        self.nl2sql = nl2sql
        self.rag = rag
        self.schema = schema
        self.validator = validator or OperationGraphValidator()
        self.llm_cfg = llm_cfg

    @staticmethod
    def _summary(answer: DBAnswer, **extra: Any) -> dict:
        result = {"summary": (answer.narrative or "")[:240]}
        result.update(extra)
        return result

    @staticmethod
    def _steps(graph: OperationGraph) -> List[dict]:
        return [{
            "tool": node.tool,
            "nodeId": node.node_id,
            "dependsOn": list(node.depends_on),
            "failurePolicy": node.failure_policy,
            "inputContract": node.input_contract,
            "outputContract": node.output_contract,
            "status": node.status,
            "input": node.input,
            "output": node.output,
            "ok": node.status == "completed",
            "error": node.error,
        } for node in graph.nodes]

    @staticmethod
    def _validate_output(node: OperationGraphNode) -> None:
        output = node.output or {}
        missing = [name for name in node.output_contract["required"] if name not in output]
        if missing:
            raise OrchestratorError(
                f"节点输出不符合契约: {node.node_id} 缺少 {', '.join(missing)}"
            )

    def _inspect_relations(self, tables: List[str], question: str = "") -> dict:
        if self.schema is None:
            raise OrchestratorError("跨表关系预检缺少 schema")
        result = SchemaRelationAnalyzer(self.schema).analyze(tables, question)
        if not result["connected"]:
            error = OrchestratorError(result["summary"])
            setattr(error, "operation_output", result)
            raise error
        return result

    def _bound_intermediate(self, answer: DBAnswer) -> DBAnswer:
        if len(answer.narrative or "") > 2000:
            answer.narrative = (answer.narrative or "")[:2000] + "…"
        if len(answer.rows) > self.MAX_INTERMEDIATE_ROWS:
            total = len(answer.rows)
            answer.rows = answer.rows[: self.MAX_INTERMEDIATE_ROWS]
            answer.narrative = (
                f"{answer.narrative}（操作图中间结果由 {total} 行截取前 "
                f"{self.MAX_INTERMEDIATE_ROWS} 行）"
            ).strip()
        if len(answer.evidence) > self.MAX_INTERMEDIATE_EVIDENCE:
            answer.evidence = answer.evidence[: self.MAX_INTERMEDIATE_EVIDENCE]
        return answer

    def _query_context(
        self,
        answers: Dict[str, DBAnswer],
        nodes: Dict[str, OperationGraphNode],
        dependencies: List[str],
    ) -> str:
        parts = []
        for node_id in dependencies:
            node = nodes[node_id]
            answer = answers.get(node_id)
            if node.tool != "query" or answer is None:
                continue
            label = str(node.parameters.get("branch_label") or node_id)
            preview = answer.rows[: self.MAX_SYNTHESIS_ROWS_PER_QUERY]
            parts.append(
                f"[{label}] {answer.narrative or '查询完成'}\n"
                f"SQL: {answer.sql or '未提供'}\n"
                f"列: {json.dumps(answer.columns, ensure_ascii=False)}\n"
                f"数据: {json.dumps(preview, ensure_ascii=False, default=str)}"
            )
        text = "\n\n".join(parts) or "（查数步骤未得到可用结果）"
        return text[: self.MAX_SYNTHESIS_CONTEXT_CHARS]

    @staticmethod
    def _answer_has_data(answer: Optional[DBAnswer]) -> bool:
        if answer is None or not answer.rows:
            return False
        if len(answer.rows) == 1 and len(answer.rows[0]) == 1:
            value = answer.rows[0][0]
            if value is None or value == "":
                return False
            if isinstance(value, (int, float)) and not isinstance(value, bool) and value == 0:
                return False
        return True

    def _synthesize(
        self,
        question: str,
        answers: Dict[str, DBAnswer],
        nodes: Dict[str, OperationGraphNode],
        dependencies: List[str],
        history: Optional[list],
    ) -> DBAnswer:
        query_items = [
            (node_id, answers[node_id])
            for node_id in dependencies
            if nodes[node_id].tool == "query" and node_id in answers
        ]
        sql_ans = query_items[0][1] if query_items else None
        rag_ans = next((
            answers[node_id]
            for node_id in dependencies
            if nodes[node_id].tool == "retrieve" and node_id in answers
        ), None)
        if sql_ans is None and rag_ans is None:
            raise OrchestratorError("没有可用于综合的上游结果")

        sql_part = self._query_context(answers, nodes, dependencies)
        rag_part = rag_ans.narrative if rag_ans is not None else "（检索步骤未得到可用结果）"
        prompt = self._COMPOSE_PROMPT.format(
            question=question,
            sql_part=sql_part,
            rag_part=rag_part,
        )
        try:
            obj = _llm_ask_json(prompt, self.llm_cfg, history=history)
            narrative = (obj.get("answer_zh") or "").strip()
        except DBAgentError:
            narrative = ""
        if not narrative:
            if query_items:
                narrative = "；".join(
                    answer.narrative or f"{node_id} 查询完成"
                    for node_id, answer in query_items
                )
            else:
                narrative = rag_ans.narrative
        datasets = [{
            "node_id": node_id,
            "label": str(nodes[node_id].parameters.get("branch_label") or node_id),
            "summary": answer.narrative,
            "sql": answer.sql,
            "columns": list(answer.columns),
            "rows": list(answer.rows),
        } for node_id, answer in query_items]
        return DBAnswer(
            kind="compose",
            narrative=narrative,
            sql=sql_ans.sql if sql_ans is not None else None,
            columns=sql_ans.columns if sql_ans is not None else [],
            rows=sql_ans.rows if sql_ans is not None else [],
            datasets=datasets if len(datasets) > 1 else [],
            evidence=rag_ans.evidence if rag_ans is not None else [],
        )

    def execute(
        self,
        graph: OperationGraph,
        history: Optional[list] = None,
    ) -> DBAnswer:
        try:
            ordered = self.validator.validate(graph)
        except OrchestratorError as exc:
            graph.status = "failed"
            graph.error = str(exc)
            return DBAnswer(
                kind="error",
                narrative=f"操作图校验失败：{exc}",
                error=str(exc),
                graph=graph.as_dict(),
            )

        graph.status = "running"
        node_by_id = {node.node_id: node for node in graph.nodes}
        answers: Dict[str, DBAnswer] = {}
        final_answer: Optional[DBAnswer] = None
        clarification_answer: Optional[DBAnswer] = None

        for node in ordered:
            dependencies = [node_by_id[node_id] for node_id in node.depends_on]
            completed_dependencies = [dep for dep in dependencies if dep.status == "completed"]
            stop_failure = any(
                dep.status in {"failed", "skipped"} and dep.failure_policy == "stop"
                for dep in dependencies
            )
            if dependencies and (stop_failure or not completed_dependencies):
                node.status = "skipped"
                node.error = "上游依赖不可用"
                continue

            if node.parameters.get("condition") == "has_data":
                upstream_answers = [answers.get(dep.node_id) for dep in completed_dependencies]
                if not any(self._answer_has_data(answer) for answer in upstream_answers):
                    node.status = "skipped"
                    node.error = "条件未满足：上游查询没有数据"
                    continue

            node.status = "running"
            try:
                if node.tool == "inspect_relations":
                    node.output = self._inspect_relations(
                        list(node.parameters.get("tables") or []),
                        node.input,
                    )
                    answer = DBAnswer(kind="schema", narrative=node.output["summary"])
                elif node.tool == "query":
                    allowed_tables = list(node.parameters.get("allowed_tables") or [])
                    answer = self.nl2sql.answer(
                        node.input,
                        history=history,
                        **({"allowed_tables": allowed_tables} if allowed_tables else {}),
                    )
                    if answer.kind == "error":
                        raise OrchestratorError(answer.error or answer.narrative)
                    if answer.kind == "clarification":
                        # 关系门禁等硬澄清：终止图执行并透传，不让综合节点消费
                        exc = OrchestratorError(answer.narrative or "查询需要补充信息")
                        setattr(exc, "operation_clarification", answer)
                        raise exc
                    answer = self._bound_intermediate(answer)
                    node.output = self._summary(
                        answer,
                        rows=len(answer.rows),
                        sql=answer.sql,
                        columns=list(answer.columns),
                        row_preview=list(answer.rows[:10]),
                    )
                elif node.tool == "retrieve":
                    answer = self.rag.answer(node.input, history=history)
                    if answer.kind == "error":
                        raise OrchestratorError(answer.error or answer.narrative)
                    answer = self._bound_intermediate(answer)
                    node.output = self._summary(answer, evidence=len(answer.evidence))
                else:
                    answer = self._synthesize(
                        node.input,
                        answers,
                        node_by_id,
                        node.depends_on,
                        history,
                    )
                    node.output = self._summary(
                        answer,
                        sources=[dep.node_id for dep in completed_dependencies],
                    )
                    final_answer = answer
                self._validate_output(node)
                node.status = "completed"
                answers[node.node_id] = answer
            except Exception as exc:  # noqa: BLE001 —— 节点失败必须被图状态完整记录
                node.status = "failed"
                node.error = str(exc)
                if getattr(exc, "operation_output", None):
                    node.output = exc.operation_output
                pending_clarification = getattr(exc, "operation_clarification", None)
                if isinstance(pending_clarification, DBAnswer):
                    clarification_answer = pending_clarification

        failed = [
            node for node in graph.nodes
            if node.status == "failed"
            or node.status == "skipped" and not node.parameters.get("condition")
        ]
        # A clarification is a hard safety boundary.  Even when a parallel
        # retrieval branch completed and synthesis produced prose, the graph
        # must not hide an unresolved query relation or metric definition.
        if clarification_answer is not None:
            graph.status = "failed"
            graph.error = clarification_answer.narrative or "查询需要补充关联条件"
            clarification_answer.steps = self._steps(graph)
            clarification_answer.graph = graph.as_dict()
            return clarification_answer
        if final_answer is None:
            graph.status = "failed"
            graph.error = "操作图未产生最终回答"
            return DBAnswer(
                kind="error",
                narrative="组合分析失败：没有可用的最终结果",
                error=graph.error,
                steps=self._steps(graph),
                graph=graph.as_dict(),
            )

        graph.status = "partial" if failed else "completed"
        final_answer.steps = self._steps(graph)
        final_answer.graph = graph.as_dict()
        return final_answer


class ToolOrchestrator:
    """兼容门面：组合意图由自研 OperationGraphPlanner/Executor 执行。"""

    def __init__(
        self,
        nl2sql: NL2SQLExecutor,
        rag: RagRetriever,
        schema: Optional[SchemaSnapshot] = None,
        semantic_catalog: Optional[SemanticCatalog] = None,
        llm_cfg: str = "default",
    ):
        self.planner = OperationGraphPlanner(schema, semantic_catalog)
        self.validator = OperationGraphValidator()
        self.executor = OperationGraphExecutor(
            nl2sql,
            rag,
            schema=schema,
            validator=self.validator,
            llm_cfg=llm_cfg,
        )

    def answer(
        self,
        question: str,
        history: Optional[list] = None,
        target_tables: Optional[List[str]] = None,
    ) -> DBAnswer:
        graph = self.planner.plan_compose(question, target_tables=target_tables)
        return self.executor.execute(graph, history=history)


# ---------------------------------------------------------------------------
# DBAgent —— 总入口（一问一答）
# ---------------------------------------------------------------------------

class DBAgent:
    """DB Agent 门面：问题 → 意图路由 → 对应执行器 → 统一 DBAnswer。"""

    def __init__(
        self,
        db_path: Optional[str] = None,
        connector: Optional[DBConnector] = None,
        semantic_entries: Optional[List[dict]] = None,
        llm_cfg: str = "default",
        sample_rows: int = 5,
        max_rows: int = 500,
        timeout_s: float = 15.0,
        reference_date: Optional[date] = None,
        allowed_tables: Optional[List[str]] = None,
        allowed_columns: Optional[Dict[str, List[str]]] = None,
        row_filters: Optional[Dict[str, List[dict]]] = None,
    ):
        if connector is not None:
            self.connector = connector
        elif db_path:
            self.connector = DBConnector(db_path)
        else:
            raise ValueError("db_path or connector required")
        self.llm_cfg = llm_cfg
        if allowed_columns and allowed_tables is None:
            raise ValueError("字段级授权必须建立在显式表级授权之上")
        if row_filters and allowed_tables is None:
            raise ValueError("row-level authorization requires an explicit table scope")
        if (allowed_tables is not None or allowed_columns or row_filters) \
                and not isinstance(self.connector, DBConnector):
            raise ValueError("表/字段级授权目前仅支持可由 SQLite authorizer 强制约束的本地数据库")
        discovered_schema = SchemaDiscovery(
            self.connector,
            sample_rows=sample_rows,
            allowed_tables=allowed_tables,
            allowed_columns=allowed_columns,
            row_filters=row_filters,
        ).discover()
        if allowed_tables is None:
            self.allowed_tables = None
            self.schema = discovered_schema
        else:
            by_folded_name = {
                name.casefold(): name for name in discovered_schema.tables
            }
            requested = []
            seen = set()
            for raw_name in allowed_tables:
                folded = str(raw_name).casefold()
                if folded not in by_folded_name:
                    raise ValueError("表级授权包含当前数据库中不存在的表")
                if folded not in seen:
                    requested.append(by_folded_name[folded])
                    seen.add(folded)
            if not requested:
                raise ValueError("表级授权至少需要一个有效表")
            self.allowed_tables = tuple(sorted(requested, key=str.casefold))
            self.schema = SchemaSnapshot(
                db_path=discovered_schema.db_path,
                tables={name: discovered_schema.tables[name] for name in self.allowed_tables},
                generated_at=discovered_schema.generated_at,
            )
        self.allowed_columns: Dict[str, tuple[str, ...]] = {}
        if allowed_columns:
            requested_tables = {
                name.casefold(): name for name in (self.allowed_tables or ())
            }
            for raw_table, raw_columns in allowed_columns.items():
                table_name = requested_tables.get(str(raw_table).casefold())
                if table_name is None:
                    raise ValueError("字段级授权只能引用当前表级授权范围内的表")
                table = self.schema.tables[table_name]
                actual_by_folded = {
                    column.name.casefold(): column.name for column in table.columns
                }
                normalized = []
                seen_columns = set()
                for raw_column in raw_columns:
                    folded = str(raw_column).casefold()
                    actual = actual_by_folded.get(folded)
                    if actual is None:
                        raise ValueError(f"字段级授权包含表 {table_name} 中不存在的字段")
                    if folded not in seen_columns:
                        normalized.append(actual)
                        seen_columns.add(folded)
                if not normalized:
                    raise ValueError(f"字段级授权表 {table_name} 至少需要一个有效字段")
                self.allowed_columns[table_name] = tuple(normalized)
        self.row_filters: Dict[str, tuple[dict, ...]] = {}
        if row_filters:
            requested_tables = {
                name.casefold(): name for name in (self.allowed_tables or ())
            }
            for folded_table, raw_filters in _normalize_row_scope(row_filters).items():
                table_name = requested_tables.get(folded_table)
                if table_name is None:
                    raise ValueError("row-level authorization references a table outside the table scope")
                self.row_filters[table_name] = tuple(dict(item) for item in raw_filters)
        self.semantic_catalog = SemanticCatalog(self.schema, semantic_entries)
        self.security = SQLSecurity(
            self.connector,
            max_rows=max_rows,
            timeout_s=timeout_s,
            allowed_tables=list(self.allowed_tables) if self.allowed_tables is not None else None,
            allowed_columns={table: list(columns) for table, columns in self.allowed_columns.items()},
            row_filters={table: list(filters) for table, filters in self.row_filters.items()},
        )
        self.conversation = BasicConversationRouter()
        self.router = IntentRouter(llm_cfg=llm_cfg)
        self.operation_planner = NaturalLanguageDatabasePlanner(self.schema, self.semantic_catalog)
        self.schema_executor = SchemaOperationExecutor(self.schema)
        self.nl2sql = NL2SQLExecutor(self.security, self.schema, llm_cfg=llm_cfg)
        self.calendar_query = DeterministicCalendarQueryExecutor(
            self.security, self.schema, self.connector,
        )
        self.multi_metric_query = DeterministicMultiMetricQueryExecutor(
            self.security, self.schema, self.connector,
        )
        self.dimension_query = DeterministicDimensionQueryExecutor(
            self.security, self.schema, self.connector, self.semantic_catalog,
        )
        self.trend_query = DeterministicTrendQueryExecutor(
            self.security, self.schema, self.connector, self.semantic_catalog,
            reference_date=reference_date,
        )
        self.rag = RagRetriever(
            self.connector,
            self.schema,
            llm_cfg=llm_cfg,
            allowed_tables=list(self.allowed_tables) if self.allowed_tables is not None else None,
            allowed_columns={table: list(columns) for table, columns in self.allowed_columns.items()},
            row_filters={table: list(filters) for table, filters in self.row_filters.items()},
        )
        self.orchestrator = ToolOrchestrator(
            self.nl2sql,
            self.rag,
            schema=self.schema,
            semantic_catalog=self.semantic_catalog,
            llm_cfg=llm_cfg,
        )
        self.write_security = WriteSecurity(
            timeout_s=timeout_s,
            allowed_tables=list(self.allowed_tables) if self.allowed_tables is not None else None,
            allowed_columns={table: list(columns) for table, columns in self.allowed_columns.items()},
            row_filters={table: list(filters) for table, filters in self.row_filters.items()},
        )
        self.write_previewer = WritePreviewer(
            self.connector,
            allowed_tables=list(self.allowed_tables) if self.allowed_tables is not None else None,
            allowed_columns={table: list(columns) for table, columns in self.allowed_columns.items()},
        )
        self.write_executor = NL2WriteExecutor(
            self.connector, self.schema,
            self.write_security, self.write_previewer,
            llm_cfg=llm_cfg,
        )
        self.structured_insert = StructuredInsertWorkflow(
            self.connector,
            self.schema,
            self.security,
            self.write_security,
            self.write_previewer,
        )
        self.structured_create_table = StructuredCreateTableWorkflow(
            self.connector,
            self.schema,
            self.write_security,
            self.write_previewer,
        )

    def write_form(
        self,
        table_name: str = "",
        intent_result: Optional[IntentResult] = None,
    ) -> DBAnswer:
        """打开 schema 约束的单行录入表单；此步不产生 SQL，也不落库。"""
        normalized_table = str(table_name or "").strip()
        operation = DatabaseOperationPlan(
            action="insert",
            mode="write",
            intent="write",
            target_tables=[normalized_table] if normalized_table else [],
            risk="medium",
            requires_confirmation=True,
            status="planned",
            confidence=intent_result.confidence if intent_result is not None else 1.0,
            reasoning=(
                intent_result.reasoning
                if intent_result is not None and intent_result.reasoning
                else "用户请求通过结构化表单新增一条记录"
            ),
        )
        try:
            answer = self.structured_insert.form(normalized_table)
        except WriteSecurityError as exc:
            answer = DBAnswer(
                kind="error",
                narrative=f"无法打开写入表单：{exc}",
                error=str(exc),
            )
        operation = self.operation_planner.enrich(operation, answer)
        answer.operation = operation.as_dict()
        answer.steps = [{
            "tool": "nl_to_database",
            "action": operation.action,
            "mode": operation.mode,
            "targets": operation.target_tables,
            "risk": operation.risk,
            "requiresConfirmation": operation.requires_confirmation,
            "status": operation.status,
        }] + (answer.steps or [])
        return answer

    def prepare_structured_insert(self, table_name: str, fields: Any) -> DBAnswer:
        """将表单单元格确定性编译成单条 INSERT，仅返回回滚预览和确认单。"""
        normalized_table = str(table_name or "").strip()
        operation = DatabaseOperationPlan(
            action="insert",
            mode="write",
            intent="write",
            target_tables=[normalized_table] if normalized_table else [],
            risk="medium",
            requires_confirmation=True,
            status="planned",
            confidence=1.0,
            reasoning="结构化单行录入经本地类型校验与回滚预览",
        )
        try:
            answer = self.structured_insert.prepare(normalized_table, fields)
        except WriteSecurityError as exc:
            answer = DBAnswer(
                kind="error",
                narrative=f"写入表单未通过校验：{exc}",
                error=str(exc),
            )
        operation = self.operation_planner.enrich(operation, answer)
        answer.operation = operation.as_dict()
        answer.steps = [{
            "tool": "structured_insert_prepare",
            "version": "1.0",
            "action": operation.action,
            "mode": operation.mode,
            "targets": operation.target_tables,
            "risk": operation.risk,
            "requiresConfirmation": operation.requires_confirmation,
            "status": operation.status,
            "model_calls": 0,
        }] + (answer.steps or [])
        return answer

    def prepare_structured_create_table(self, table_name: str, columns: Any) -> DBAnswer:
        """将受控表单确定性编译为单条 CREATE TABLE，只返回回滚预览。"""
        normalized_table = str(table_name or "").strip()
        operation = DatabaseOperationPlan(
            action="create",
            mode="write",
            intent="write",
            target_tables=[normalized_table] if normalized_table else [],
            risk="high",
            requires_confirmation=True,
            status="planned",
            confidence=1.0,
            reasoning="自定义建表经过本地类型白名单、单语句校验与回滚预览",
        )
        try:
            answer = self.structured_create_table.prepare(normalized_table, columns)
        except WriteSecurityError as exc:
            answer = DBAnswer(
                kind="error",
                narrative=f"建表表单未通过校验：{exc}",
                error=str(exc),
            )
        operation = self.operation_planner.enrich(operation, answer)
        answer.operation = operation.as_dict()
        answer.steps = [{
            "tool": "structured_create_table_prepare",
            "version": "1.0",
            "action": operation.action,
            "mode": operation.mode,
            "targets": operation.target_tables,
            "risk": operation.risk,
            "requiresConfirmation": operation.requires_confirmation,
            "status": operation.status,
            "model_calls": 0,
        }] + (answer.steps or [])
        return answer

    def ask(
        self,
        question: str,
        history: Optional[list] = None,
        clarification: Optional[dict] = None,
    ) -> DBAnswer:
        """总入口：意图路由 → 分发执行。history 为会话内多轮上下文（可选）。"""
        raw_sql_rejection = OriginalSQLRequestGuard.reject_reason(question)
        if raw_sql_rejection:
            return DBAnswer(
                kind="error",
                narrative=raw_sql_rejection,
                error="original_request_contains_multiple_sql_statements",
                operation=DatabaseOperationPlan(
                    action="reject",
                    mode="safety",
                    intent="query",
                    risk="high",
                    requires_confirmation=False,
                    status="failed",
                    reasoning="原始请求在模型路由前命中多语句执行边界",
                ).as_dict(),
                steps=[{
                    "tool": "original_sql_request_guard",
                    "version": "1.0",
                    "status": "rejected",
                    "model_calls": 0,
                }],
            )
        conversation_answer = self.conversation.answer(
            question,
            clarification=clarification,
        )
        if conversation_answer is not None:
            return conversation_answer
        resolved_question = self.operation_planner.resolve_followup(question, history, clarification)
        semantic = self.semantic_catalog.resolve(resolved_question)
        execution_question = semantic.resolved_question
        schema_plan = self.operation_planner.plan_schema(resolved_question)
        if schema_plan is not None:
            ans = self.schema_executor.answer(schema_plan)
            schema_plan = self.operation_planner.enrich(schema_plan, ans)
            ans.operation = schema_plan.as_dict()
            ans.semantic = semantic.as_dict() if semantic.matches else None
            ans.steps = [{
                "tool": "nl_to_database",
                "action": schema_plan.action,
                "mode": schema_plan.mode,
                "targets": schema_plan.target_tables,
                "status": schema_plan.status,
            }]
            return ans

        schema_context = self.schema.compact()
        semantic_context = self.semantic_catalog.prompt_context()
        if semantic_context:
            schema_context += "\n\n业务语义目录：\n" + semantic_context
        ir = self.router.classify(execution_question, schema_context, history=history)
        operation = self.operation_planner.from_intent(resolved_question, ir)
        if operation.mode == "write" and isinstance(self.connector, RemoteDBConnector):
            operation.status = "failed"
            operation.reasoning = (
                "远程 MySQL/PostgreSQL 连接当前是只读产品边界；"
                "未生成、预览或执行写 SQL"
            )
            return DBAnswer(
                kind="error",
                narrative=(
                    "当前 MySQL/PostgreSQL 数据源仅支持只读问答，"
                    "这条写入请求未执行。"
                ),
                error="remote_database_write_not_supported",
                operation=operation.as_dict(),
                steps=[{
                    "tool": "remote_read_only_boundary",
                    "version": "1.0",
                    "status": "rejected",
                    "model_calls": 0,
                }],
            )
        clarification = self.operation_planner.clarification_for(resolved_question, operation)
        if self.structured_insert.should_offer(operation, clarification, ir):
            selected_table = operation.target_tables[0] if len(operation.target_tables) == 1 else ""
            answer = self.write_form(selected_table, intent_result=ir)
            answer.semantic = semantic.as_dict() if semantic.matches else None
            answer.steps = [
                {
                    "tool": "intent",
                    "intent": ir.intent,
                    "interaction": ir.interaction,
                    "target_table": selected_table,
                    "source": ir.source,
                    "confidence": round(ir.confidence, 2),
                },
                *(answer.steps or []),
            ]
            return answer
        if not operation.target_tables:
            if clarification is not None and clarification.get("missing") == "target_table":
                mapped = self.operation_planner.llm_map_target_table(
                    execution_question, schema_context, llm_cfg=self.llm_cfg,
                )
                if mapped:
                    operation.target_tables = [mapped]
                    operation.reasoning = (
                        f"{operation.reasoning or ''}；目标表由大模型映射为 {mapped}"
                    ).strip()
        # 列映射放行：目标表已明确但澄清仍卡在“目标字段/新增字段值”时，
        # 用 LLM 从真实字段中消歧中文业务字段，并把映射写回问题（保留字段=值结构）
        # 后重新澄清；映射未推进澄清时回退原文，避免把问题改坏（fail-closed）。
        clarification = self.operation_planner.clarification_for(resolved_question, operation)
        if clarification is not None and clarification.get("missing") in ("target_field", "record_values") \
                and len(operation.target_tables) == 1:
            table_name = operation.target_tables[0]
            pre_mapping_question = resolved_question
            pre_mapping_missing = clarification.get("missing")
            mapped_columns = self.operation_planner.llm_map_target_columns(
                execution_question, table_name, llm_cfg=self.llm_cfg,
            )
            if mapped_columns:
                operation.reasoning = (
                    f"{operation.reasoning or ''}；业务字段由大模型映射为 "
                    + ", ".join(f"{item['term']}→{item['column']}" for item in mapped_columns)
                ).strip()
                resolved_question = self.operation_planner.rewrite_with_mapped_columns(
                    resolved_question, table_name, mapped_columns,
                )
                execution_question = resolved_question
                clarification = self.operation_planner.clarification_for(
                    resolved_question, operation,
                )
                if clarification is not None and clarification.get("missing") == pre_mapping_missing:
                    resolved_question = pre_mapping_question
                    execution_question = pre_mapping_question
        # 自然语言写请求规范化（列映射兜底之后）：自由表述
        # （“把张三的城市改成北京”“删除客户张三”）缺对象名词或字段/条件时，
        # 让 LLM 结构化解析并确定性拼装为规范指令；表/字段只能取真实 schema
        # 名称并经校验，未推进澄清则保持原澄清（fail-closed）。
        if clarification is not None and operation.mode == "write" \
                and clarification.get("missing") in (
                    "target_table", "object_name", "operation_type",
                    "target_field", "new_value", "filter_condition",
                    "record_values", "object_definition", "schema_change_definition",
                ):
            rewritten = self.operation_planner.llm_rewrite_write_request(
                execution_question, llm_cfg=self.llm_cfg,
            )
            if rewritten is None and execution_question != question:
                # 列映射可能已改写问题；结构化解析优先用用户原话
                rewritten = self.operation_planner.llm_rewrite_write_request(
                    question, llm_cfg=self.llm_cfg,
                )
            if rewritten and rewritten.strip() != resolved_question.strip():
                rewritten_op = self.operation_planner.from_intent(
                    rewritten,
                    IntentResult(intent="write", confidence=0.9, reasoning="自然语言写请求经规范化改写"),
                )
                rewritten_cl = self.operation_planner.clarification_for(rewritten, rewritten_op)
                if rewritten_cl is None or rewritten_cl.get("missing") != clarification.get("missing"):
                    resolved_question = rewritten
                    execution_question = rewritten
                    operation = rewritten_op
                    clarification = rewritten_cl
        if clarification is not None:
            answer = self.operation_planner.clarification_answer(operation, clarification)
            answer.semantic = semantic.as_dict() if semantic.matches else None
            return answer
        metric_answer = (
            self.multi_metric_query.answer(resolved_question, semantic)
            if ir.intent in {"query", "compose"} else None
        )
        if metric_answer is not None:
            if ir.intent != "query":
                ir = IntentResult(
                    intent="query",
                    confidence=1.0,
                    reasoning="多个受控同表指标由确定性单次聚合执行",
                )
                operation = self.operation_planner.from_intent(resolved_question, ir)
            ans = metric_answer
        elif ir.intent == "query":
            ans = self.trend_query.answer(resolved_question, semantic)
            if ans is None:
                ans = self.dimension_query.answer(resolved_question, semantic)
            if ans is None:
                ans = self.calendar_query.answer(resolved_question, semantic)
            if ans is None:
                ans = self.nl2sql.answer(execution_question, history=history)
        elif ir.intent == "compose":
            ans = self.orchestrator.answer(
                execution_question,
                history=history,
                target_tables=operation.target_tables,
            )
        elif ir.intent == "write":
            ans = self.write_executor.prepare(execution_question, history=history)
        else:  # retrieve / 降级
            ans = self.rag.answer(execution_question, history=history)
        operation = self.operation_planner.enrich(operation, ans)
        ans.operation = operation.as_dict()
        ans.semantic = semantic.as_dict() if semantic.matches else None
        steps = [
            {
                "tool": "nl_to_database",
                "action": operation.action,
                "mode": operation.mode,
                "targets": operation.target_tables,
                "risk": operation.risk,
                "requiresConfirmation": operation.requires_confirmation,
                "status": operation.status,
            },
            *([{
                "tool": "semantic_catalog",
                "matches": len(semantic.matches),
                "terms": [item["term"] for item in semantic.matches],
                "status": "resolved",
            }] if semantic.matches else []),
            {
                "tool": "intent",
                "intent": ir.intent,
                "interaction": ir.interaction,
                "target_table": ir.target_table,
                "source": ir.source,
                "confidence": round(ir.confidence, 2),
            },
        ]
        ans.steps = steps + (ans.steps or [])
        return ans

    def confirm_write(self, confirm_id: str, approve: bool = True) -> DBAnswer:
        """用户对写提案表态：approve=True 正式执行落库；False 作废（Human-in-the-loop）。"""
        proposal = WRITE_REGISTRY.resolve(
            confirm_id,
            expected_db_path=self.connector.db_path,
        )
        if proposal is None:
            return DBAnswer(
                kind="error",
                narrative="确认单不存在、已过期或不属于当前数据库",
                error="confirm_id not found, expired, or database mismatch",
                operation=DatabaseOperationPlan(
                    action="mutate", mode="write", intent="write", risk="high",
                    requires_confirmation=True, status="failed",
                    reasoning="确认单无效或与当前数据库不匹配",
                ).as_dict(),
            )
        operation = DatabaseOperationPlan(
            action=proposal.kind.lower(),
            mode="write",
            intent="write",
            target_tables=[proposal.table] if proposal.table else [],
            risk="high" if proposal.dangerous or proposal.kind in {"DELETE", "CREATE", "ALTER", "DROP"} else "medium",
            requires_confirmation=True,
            status="planned",
            confidence=1.0,
            reasoning="已通过确认单绑定到当前数据库",
            sql=proposal.sql,
        )
        if not approve:
            operation.status = "cancelled"
            return DBAnswer(
                kind="write_result", narrative="已取消写操作", confirm_id=confirm_id,
                operation=operation.as_dict(),
            )
        # 批准后、执行前再校验一次（防超时窗口期 schema/权限变化）
        try:
            self.write_security.validate_write(proposal.sql)
        except WriteSecurityError as e:
            operation.status = "failed"
            return DBAnswer(
                kind="error", narrative=f"写操作被安全拦截：{e}", error=str(e),
                operation=operation.as_dict(),
            )
        conn = self.connector.connect_rw()
        rowcount = 0
        try:
            self.write_previewer._install_table_authorizer(conn)
            conn.execute("BEGIN")
            cur = conn.execute(proposal.sql)
            rowcount = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
            conn.commit()
        except sqlite3.Error as e:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            operation.status = "failed"
            return DBAnswer(
                kind="error", narrative=f"写入失败，已回滚：{e}", error=str(e),
                operation=operation.as_dict(),
            )
        finally:
            self.connector.close(conn)
        operation.status = "executed"
        return DBAnswer(
            kind="write_result",
            narrative=f"写操作已完成：{proposal.summary_zh}",
            sql=proposal.sql,
            confirm_id=confirm_id,
            write={"affected": rowcount, "kind": proposal.kind, "table": proposal.table},
            operation=operation.as_dict(),
        )


# ---------------------------------------------------------------------------
# Purpose-built model gateway integration
# ---------------------------------------------------------------------------

_HISTORY_MAX_MSGS = 14  # 会话内多轮历史最多注入的消息条数（约 7 轮）
_ACTIVE_CANCEL_EVENT: contextvars.ContextVar[Optional[threading.Event]] = (
    contextvars.ContextVar("dbagent_active_cancel_event", default=None)
)


@contextmanager
def cancellation_scope(cancel_event: Optional[threading.Event]):
    """Bind one bridge run's cooperative cancellation signal to LLM calls."""
    token = _ACTIVE_CANCEL_EVENT.set(cancel_event)
    try:
        yield
    finally:
        _ACTIVE_CANCEL_EVENT.reset(token)


def _llm_ask(prompt: str, cfg_name: str = "default", history: Optional[list] = None) -> str:
    """Send a bounded prompt through the selected local model profile.

    history: 会话内多轮上下文 [{"role": "user"/"assistant", "content": str}, ...],
             非空时拼接到 prompt 前缀，供 LLM 理解指代/追问。
    """
    if history:
        hist_lines = []
        for msg in history[-_HISTORY_MAX_MSGS:]:
            role = "用户" if msg.get("role") == "user" else "助手"
            content = str(msg.get("content") or "").strip()[:400]
            if content:
                hist_lines.append(f"{role}: {content}")
        if hist_lines:
            prompt = (
                "以下是本次会话中此前的对话历史（仅作上下文参考，可能与本问题相关）：\n"
                + "\n".join(hist_lines)
                + "\n\n"
                + "请结合历史理解当前问题并回答。\n\n"
                + prompt
            )
    import sys
    from pathlib import Path

    here = str(Path(__file__).resolve().parent)
    if here not in sys.path:
        sys.path.insert(0, here)
    import model_gateway

    return model_gateway.generate_text(
        prompt, cfg_name, cancel_event=_ACTIVE_CANCEL_EVENT.get(),
    )


def _looks_english(s: str) -> bool:
    """粗略判断一段文本是否以英文为主（用于中文兜底翻译）。含较多中文则视为中文，避免误判 SQL/数据。"""
    if not s:
        return False
    cjk = sum(1 for c in s if "\u4e00" <= c <= "\u9fff")
    letters = [c for c in s if c.isalpha()]
    if not letters:
        return False
    en = sum(1 for c in letters if "a" <= c.lower() <= "z")
    if cjk >= 2 and cjk / (cjk + en) > 0.2:
        return False  # 有明显中文，不按英文处理
    return en / len(letters) > 0.6


# 常用列名的确定性中文表头。未知列保持物理名称，避免为表头额外调用模型。
_COL_ZH = {
    # 通用
    "id": "编号", "name": "名称", "title": "标题", "year": "年份", "url": "链接",
    "status": "状态", "type": "类型", "date": "日期", "count": "数量", "total": "总计",
    "summary": "摘要", "abstract": "摘要", "field": "领域", "weight": "权重",
    "confidence": "置信度", "note": "备注", "remark": "备注", "desc": "描述",
    "description": "描述", "text": "内容", "content": "内容", "value": "值",
    "score": "分数", "rank": "排名", "level": "级别", "category": "类别",
    "org": "机构", "organization": "机构", "department": "院系", "school": "学院",
    "author": "作者", "email": "邮箱", "phone": "电话", "address": "地址",
    "created_at": "创建时间", "updated_at": "更新时间", "deleted_at": "删除时间",
    "start_date": "开始日期", "end_date": "结束日期", "begin_time": "开始时间",
    "end_time": "结束时间", "time": "时间", "duration": "时长", "age": "年龄",
    "gender": "性别", "nationality": "国籍", "city": "城市", "province": "省份",
    "country": "国家", "language": "语言", "version": "版本", "size": "大小",
    "data": "数据", "dataset": "数据集", "result": "结果", "results": "结果",
    "source": "来源", "target": "目标", "key": "键", "label": "标签", "tag": "标签",
    "tags": "标签", "keyword": "关键词", "keywords": "关键词", "query": "查询",
    "sql": "SQL", "password": "密码", "username": "用户名", "user": "用户",
    "creator": "创建人", "owner": "负责人", "role": "角色", "permission": "权限",
    "start": "开始", "end": "结束", "min": "最小值", "max": "最大值",
    "avg": "平均值", "sum": "总和", "cnt": "数量", "num": "数量", "n": "数量",
    "ratio": "比例", "percent": "百分比", "rate": "比率", "price": "价格",
    "amount": "金额", "quantity": "数量", "unit_price": "单价", "currency": "货币",
    "file": "文件", "filename": "文件名", "path": "路径", "location": "位置",
    "latitude": "纬度", "longitude": "经度", "coordinate": "坐标", "area": "面积",
    "row_num": "行号", "table_name": "表名", "column_name": "列名", "schema_name": "模式名",
    "database_name": "数据库名", "file_path": "文件路径", "file_size": "文件大小",
    "file_type": "文件类型", "uploaded_at": "上传时间", "processed_at": "处理时间",
    "error_message": "错误信息", "error_code": "错误码", "success": "是否成功",
    "failed": "是否失败", "retry_count": "重试次数", "attempts": "尝试次数",
    "thread_count": "线程数", "cpu_usage": "CPU使用率", "memory_usage": "内存使用率",
    "version_code": "版本号", "build_number": "构建号", "release_date": "发布日期",
    "official_url": "官网", "docs_url": "文档链接", "github_url": "GitHub链接",
    "demo_url": "演示链接", "download_url": "下载链接", "cover_image": "封面图",
    "logo": "标志", "icon": "图标", "color": "颜色", "theme": "主题",
    "session_id": "会话ID", "run_id": "运行ID", "task_id": "任务ID",
    "job_id": "作业ID", "request_id": "请求ID", "order_id": "订单ID",
    "user_id": "用户ID", "student_id": "学生ID", "teacher_id": "教师ID",
    "paper_id": "论文ID", "event_id": "事件ID",
    "claim": "断言", "claim_text": "断言文本", "evidence": "证据",
    "evidence_text": "证据文本", "evidence_url": "证据链接", "verified_by": "核验人",
    "verified_at": "核验时间", "accuracy": "准确率", "precision": "精确率",
    "recall": "召回率", "f1": "F1值", "latency_ms": "延迟(ms)", "elapsed": "耗时",
}


def _col_zh(name: str, cfg_name: str = "default") -> str:
    """把常用英文列名转中文表头；未知名称原样返回。"""
    if not name:
        return name
    if _looks_english(name) is False and any("\u4e00" <= c <= "\u9fff" for c in name):
        return name  # 已是中文
    base = name
    if "." in name:  # schema.table.col / table.col
        base = name.rsplit(".", 1)[-1]
    if base in _COL_ZH:
        return _COL_ZH[base]
    return name


def _ensure_zh(text: str, cfg_name: str = "default") -> str:
    """若 LLM 输出英文叙述，自动再请求一次翻译为简体中文；失败/已中文则原样返回。"""
    if not text or not _looks_english(text):
        return text
    try:
        tr = _llm_ask(
            "请把以下英文内容翻译成简体中文。只输出翻译后的中文，不要任何解释、引号或英文原文：\n\n"
            + text,
            cfg_name,
        )
        tr = (tr or "").strip().strip('"\'')
        if tr and not _looks_english(tr):
            return tr
    except Exception:
        pass
    return text


def _llm_ask_json(prompt: str, cfg_name: str = "default", history: Optional[list] = None) -> dict:
    """LLM 输出 JSON 并严格解析（校验器风格：解析失败抛异常，不静默）。"""
    raw = _llm_ask(prompt, cfg_name, history=history)
    stripped = (raw or "").strip()
    http_error = re.match(r"^!!!Error:\s*HTTP\s+(\d{3})\b", stripped, re.IGNORECASE)
    if http_error:
        raise LLMServiceError(f"LLM 服务请求失败（HTTP {http_error.group(1)}）")
    terminal_error = re.match(r"^!!!Error:\s*([^:\r\n]+)", stripped)
    if terminal_error:
        reason = terminal_error.group(1).strip()
        raise LLMServiceError(f"LLM 服务调用失败（{reason}）")
    stream_error = re.search(r"\[!!!\s*流异常中断\s+([A-Za-z_][\w.]*)", stripped)
    if stream_error:
        raise LLMServiceError(f"LLM 服务响应中断（{stream_error.group(1)}）")
    # 提取第一个 JSON 对象（容忍 markdown 围栏 / 前后废话）
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if not m:
        raise DBAgentError(f"LLM 输出不含 JSON: {raw[:200]}")
    try:
        obj = json.loads(m.group(0))
    except json.JSONDecodeError as e:
        raise DBAgentError(f"LLM JSON 解析失败: {e} | raw={raw[:200]}") from e
    # 中文兜底：叙述类字段若为英文，自动翻译为简体中文（sql 等代码字段不翻译）
    for k in ("summary_zh", "answer_zh", "reasoning", "narrative"):
        if isinstance(obj.get(k), str):
            obj[k] = _ensure_zh(obj[k], cfg_name)
    return obj


if __name__ == "__main__":
    # 骨架自检：可导入、可实例化（需真实 db 路径才完整工作）
    print("dbagent_core skeleton OK")
    print("classes:", [c for c in dir() if c[0].isupper() and c not in ("DBConnector", "SchemaDiscovery", "SQLSecurity", "NL2SQLExecutor", "RagRetriever", "IntentRouter", "ToolOrchestrator", "DBAgent")])
