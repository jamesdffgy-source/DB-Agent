# Changelog

All notable user-facing changes are recorded here. Versions follow semantic versioning while the project is in its initial public phase.

## Unreleased

### Added

- Added a bounded read-only observe/replan loop for unresolved database
  questions. The model can inspect real schema, retrieve raw evidence, issue a
  natural-language query, finish, or stop within local step, tool, time, row,
  authorization, repetition, and entity-grounding limits. Writes never enter
  this loop.
- Added a compact Chinese/English selector beside the DBQuill wordmark. The
  interface locale follows the saved local preference, defaults to English on
  first launch, and translates static and dynamically rendered application controls
  without sending interface text to a translation service.
- Added live upload progress with a separate database-inspection phase.

### Fixed

- Read routing now preserves model-proposed entities and read targets only after
  grounding them in the original question and real schema. When a router omits
  them, exact values from canonical identity columns can supply the execution
  anchor; later retrievals cannot silently drift to a pronoun or another entity.
- Chinese evidence keywords now interleave 2/3/4-character candidates by text
  position, preventing the bounded keyword budget from being exhausted by
  unmatched 4-character fragments before a real short name is tried.
- Entity-centric questions now retrieve exact names before broad terms, scan
  descriptive columns independently of physical column order, and exclude
  unrelated records once an exact entity has been found.
- Short and chained follow-ups such as `是什么`, `那他的研究方向呢`, and
  `证据呢` now inherit the most recent complete user topic. Empty NL-to-SQL
  results can fall back to bounded cross-table evidence only when the original
  user language contains an exact entity.
- Referential corrections such as `我的意思是他担任的工作` now skip earlier
  dependent turns and recover the last complete named subject. A bare request
  for a person's `工作` asks whether the user means role/employment or research
  work/results instead of silently choosing one meaning.
- Canonical identity/profile tables with explicit name and profile fields are
  searched before audit summaries and other free-text mention tables, keeping
  bounded evidence focused on authoritative role and biography fields.
- Multi-part entity evidence questions no longer guess a cross-table join merely
  because they ask for status, known/unknown fields, and evidence sufficiency.
  Null and `unresolved` fields are reported as missing evidence rather than facts.
- Replaced renderer-side Base64/JSON uploads with bounded multipart streaming.
  Large databases no longer require several full in-memory copies in the desktop
  page; incomplete and over-limit uploads are discarded before publication.
- Added a launcher/bridge protocol handshake so an updated desktop page cannot
  reuse an older JSON-only bridge. UTF-8 multipart filenames are decoded before
  safe-name normalization, preserving readable non-ASCII database names.

## 0.2.0 - 2026-08-27

### Added

- Added opt-in controlled `INSERT`, bounded `UPDATE`, and bounded `DELETE` for
  MySQL and PostgreSQL. Read traffic keeps a separate physical read-only session;
  writes use rollback preview, one-time confirmation, transactional commit, and audit.
- Added a remote connection-mode selector. Read-only remains the default, and remote
  DDL remains blocked because it cannot be previewed with equivalent rollback safety.

### Changed

- Renamed the product and repository to **DBQuill**, with the descriptor
  **Natural-Language Database Agent**.
- Renamed the launcher, core module, startup command, release archives, demo
  database, icons, screenshots, and public export filenames to the DBQuill brand.
- Added DBQuill environment variables and HTTP headers while retaining v0.1
  environment, header, cookie, semantic-import, audit-artifact, credential, and
  authorization identifiers as compatibility inputs.

### Compatibility

- Existing databases, role tokens, scoped credentials, sessions, audit ledgers,
  semantic catalogs, and previously generated timezone SQL remain readable.
- The rename does not relax the read-only default, authorization scopes, write
  previews, one-time confirmation, audit integrity, or query limits.

## 0.1.0 - 2026-08-26

### Added

- Natural-language database operation planning with a typed relational contract.
- Complete SQLite workflow for schema exploration, retrieval, charts, semantic definitions, scheduling, and confirmed writes.
- Read-only MySQL 8.4 and PostgreSQL 17 paths with schema discovery, bounded execution, and timeout handling.
- Table-aware insert forms, change previews, one-time confirmation, local authorization, and append-only audit records.
- CSV and `.xlsx` import, session management, chart caching, and local model-profile settings.
- Hash-locked Windows installation, clean-machine diagnostics, CI gate, and authenticated startup smoke test.
- Patched `aiohttp` 3.14.3 baseline; the release lock excludes the vulnerable 3.14.1 build reported during public dependency scanning.

### Known limits

- Distribution is source-first and requires Windows x64, Python 3.12, and WebView2.
- MySQL and PostgreSQL controlled DML is new in 0.2.0 and has regression-level
  transaction coverage; live vendor write-matrix evidence is not yet published.
- macOS, Linux, Windows on ARM, and legacy `.xls` imports are not verified release targets.
