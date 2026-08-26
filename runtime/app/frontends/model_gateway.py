"""Purpose-built OpenAI-compatible model transport for DBQuill.

This module has one job: turn a prompt into text through a selected local model
profile.  It does not expose a tool runtime, autonomous loop, or general-purpose
agent session.
"""
from __future__ import annotations

import contextlib
import json
import threading
import time
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

import requests

from model_profiles import ModelProfileStore


_APP_ROOT = Path(__file__).resolve().parent.parent
_profiles = ModelProfileStore(_APP_ROOT)
_TRANSIENT_HTTP_STATUS = {
    408, 409, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 525,
    526, 527, 529,
}
_usage_lock = threading.Lock()
_usage_by_thread: dict[str, dict[str, int]] = {}
_usage_total = {"input": 0, "output": 0, "cache_read": 0}


def get_profile(profile_key: str) -> dict:
    """Return one validated local profile, resolving ``default`` to the first."""
    return _profiles.get_runtime_profile(profile_key)


def token_usage_by_thread() -> dict[str, dict[str, int]]:
    with _usage_lock:
        return {name: dict(values) for name, values in _usage_by_thread.items()}


def token_usage_total() -> dict[str, int]:
    with _usage_lock:
        return dict(_usage_total)


def reset_token_usage() -> None:
    with _usage_lock:
        _usage_by_thread.clear()
        _usage_total.update({"input": 0, "output": 0, "cache_read": 0})


def _record_usage(payload: Any) -> None:
    if not isinstance(payload, dict):
        return
    input_tokens = int(payload.get("prompt_tokens") or payload.get("input_tokens") or 0)
    output_tokens = int(payload.get("completion_tokens") or payload.get("output_tokens") or 0)
    details = payload.get("prompt_tokens_details")
    cache_read = int(details.get("cached_tokens") or 0) if isinstance(details, dict) else 0
    thread_name = threading.current_thread().name or "main"
    with _usage_lock:
        counters = _usage_by_thread.setdefault(
            thread_name, {"input": 0, "output": 0, "cache_read": 0},
        )
        counters["input"] += input_tokens
        counters["output"] += output_tokens
        counters["cache_read"] += cache_read
        _usage_total["input"] += input_tokens
        _usage_total["output"] += output_tokens
        _usage_total["cache_read"] += cache_read


def _operation_url(base_url: str, operation: str) -> str:
    base = str(base_url or "").strip().rstrip("/")
    suffix = str(operation or "").strip().strip("/")
    if not base:
        return suffix
    if base.endswith(f"/{suffix}"):
        return base
    return f"{base}/{suffix}"


def _is_cancelled(event: Optional[threading.Event]) -> bool:
    return event is not None and event.is_set()


def _pause(delay: float, event: Optional[threading.Event]) -> bool:
    if event is None:
        time.sleep(delay)
        return False
    return event.wait(delay)


def _chat_json_text(document: dict) -> str:
    _record_usage(document.get("usage"))
    choices = document.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            str(part.get("text") or "")
            for part in content
            if isinstance(part, dict) and part.get("type") in {"text", "output_text"}
        )
    return ""


def _responses_json_text(document: dict) -> str:
    _record_usage(document.get("usage"))
    direct = document.get("output_text")
    if isinstance(direct, str):
        return direct
    fragments: list[str] = []
    for item in document.get("output") or []:
        if not isinstance(item, dict):
            continue
        for part in item.get("content") or []:
            if isinstance(part, dict) and part.get("type") in {"output_text", "text"}:
                fragments.append(str(part.get("text") or ""))
    return "".join(fragments)


def _sse_text(
    lines: Iterable[bytes | str],
    *,
    mode: str,
    deadline: float,
    cancel_event: Optional[threading.Event],
) -> str:
    fragments: list[str] = []
    for raw_line in lines:
        if _is_cancelled(cancel_event):
            raise InterruptedError("cancelled")
        if time.monotonic() >= deadline:
            raise requests.Timeout("model request deadline exceeded")
        line = raw_line.decode("utf-8", errors="replace") if isinstance(raw_line, bytes) else raw_line
        if not line.startswith("data:"):
            continue
        encoded = line[5:].strip()
        if not encoded or encoded == "[DONE]":
            if encoded == "[DONE]":
                break
            continue
        try:
            event = json.loads(encoded)
        except json.JSONDecodeError:
            continue
        _record_usage(event.get("usage"))
        if mode == "responses":
            if event.get("type") == "response.output_text.delta":
                fragments.append(str(event.get("delta") or ""))
            continue
        choices = event.get("choices")
        if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
            continue
        delta = choices[0].get("delta")
        if isinstance(delta, dict) and isinstance(delta.get("content"), str):
            fragments.append(delta["content"])
    return "".join(fragments)


def _retry_delay(response: Any, attempt: int) -> float:
    header_value = None
    if response is not None:
        try:
            header_value = float((response.headers or {}).get("retry-after"))
        except (TypeError, ValueError):
            header_value = None
    if header_value is not None:
        return max(0.25, min(header_value, 30.0))
    return min(30.0, 0.75 * (2 ** attempt))


def _request_text(
    profile: dict,
    url: str,
    headers: dict,
    body: dict,
    *,
    use_stream: bool,
    cancel_event: Optional[threading.Event] = None,
) -> Iterator[str]:
    attempts = int(profile.get("retry_count", 1)) + 1
    total_timeout = max(10.0, min(float(profile.get("total_timeout", 600)), 1800.0))
    deadline = time.monotonic() + total_timeout
    mode = str(profile.get("api_mode") or "chat_completions")
    proxies = None
    if profile.get("proxy_url"):
        proxies = {"http": profile["proxy_url"], "https": profile["proxy_url"]}

    for attempt in range(attempts):
        if _is_cancelled(cancel_event):
            yield "!!!Error: Cancelled"
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            yield "!!!Error: TotalTimeout"
            return
        response = None
        watcher_done = threading.Event()
        watcher = None
        try:
            response = requests.post(
                url,
                headers=headers,
                json=body,
                stream=use_stream,
                timeout=(
                    max(0.1, min(float(profile.get("connect_timeout", 10)), remaining)),
                    max(0.1, min(float(profile.get("read_timeout", 120)), remaining)),
                ),
                proxies=proxies,
                verify=bool(profile.get("verify_tls", True)),
            )
            if cancel_event is not None:
                def close_after_cancel() -> None:
                    while not watcher_done.wait(0.05):
                        if cancel_event.is_set():
                            with contextlib.suppress(Exception):
                                response.close()
                            return

                watcher = threading.Thread(
                    target=close_after_cancel,
                    name="DBQuill-Model-Cancel",
                    daemon=True,
                )
                watcher.start()
            if response.status_code >= 400:
                if response.status_code in _TRANSIENT_HTTP_STATUS and attempt + 1 < attempts:
                    delay = _retry_delay(response, attempt)
                    if time.monotonic() + delay >= deadline:
                        yield "!!!Error: TotalTimeout"
                        return
                    if _pause(delay, cancel_event):
                        yield "!!!Error: Cancelled"
                        return
                    continue
                yield f"!!!Error: HTTP {response.status_code}"
                return
            if use_stream:
                text = _sse_text(
                    response.iter_lines(), mode=mode, deadline=deadline,
                    cancel_event=cancel_event,
                )
            else:
                document = response.json()
                text = _responses_json_text(document) if mode == "responses" else _chat_json_text(document)
                if time.monotonic() >= deadline:
                    raise requests.Timeout("model request deadline exceeded")
            if _is_cancelled(cancel_event):
                yield "!!!Error: Cancelled"
                return
            if text:
                yield text
            return
        except InterruptedError:
            yield "!!!Error: Cancelled"
            return
        except (
            requests.Timeout,
            requests.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ) as exc:
            if _is_cancelled(cancel_event):
                yield "!!!Error: Cancelled"
                return
            if time.monotonic() >= deadline:
                yield "!!!Error: TotalTimeout"
                return
            if attempt + 1 >= attempts:
                yield f"!!!Error: {type(exc).__name__}"
                return
            delay = _retry_delay(response, attempt)
            if time.monotonic() + delay >= deadline:
                yield "!!!Error: TotalTimeout"
                return
            if _pause(delay, cancel_event):
                yield "!!!Error: Cancelled"
                return
        except (requests.RequestException, ValueError, TypeError, json.JSONDecodeError) as exc:
            yield f"!!!Error: {type(exc).__name__}"
            return
        finally:
            watcher_done.set()
            if watcher is not None:
                watcher.join(timeout=0.1)
            if response is not None:
                with contextlib.suppress(Exception):
                    response.close()


def stream_text(
    prompt: str,
    profile_key: str = "default",
    *,
    stream: Optional[bool] = None,
    cancel_event: Optional[threading.Event] = None,
) -> Iterator[str]:
    """Yield model text for one prompt using a selected local profile."""
    if _is_cancelled(cancel_event):
        yield "!!!Error: Cancelled"
        return
    profile = get_profile(profile_key)
    model = str(profile.get("model") or "").strip()
    api_key = str(profile.get("api_key") or "").strip()
    base_url = str(profile.get("base_url") or "").strip()
    if not model or not api_key or not base_url:
        yield "!!!Error: ModelProfileIncomplete"
        return
    mode = str(profile.get("api_mode") or "chat_completions")
    use_stream = bool(profile.get("stream", True)) if stream is None else bool(stream)
    operation = "responses" if mode == "responses" else "chat/completions"
    url = _operation_url(base_url, operation)
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream" if use_stream else "application/json",
    }
    if mode == "responses":
        body: dict[str, Any] = {"model": model, "input": prompt, "stream": use_stream}
    else:
        body = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": use_stream,
        }
        if use_stream:
            body["stream_options"] = {"include_usage": True}
    if profile.get("temperature") not in (None, ""):
        body["temperature"] = float(profile["temperature"])
    if profile.get("reasoning_effort"):
        body["reasoning_effort"] = profile["reasoning_effort"]
    if profile.get("max_tokens"):
        token_field = "max_completion_tokens" if model.lower().startswith(("gpt-5", "o1", "o3", "o4")) else "max_tokens"
        body[token_field] = int(profile["max_tokens"])
    yield from _request_text(
        profile, url, headers, body, use_stream=use_stream,
        cancel_event=cancel_event,
    )


def generate_text(
    prompt: str,
    profile_key: str = "default",
    *,
    stream: Optional[bool] = None,
    cancel_event: Optional[threading.Event] = None,
) -> str:
    return "".join(
        stream_text(
            prompt, profile_key, stream=stream, cancel_event=cancel_event,
        )
    )
