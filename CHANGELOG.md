# Changelog

All notable user-facing changes are recorded here. Versions follow semantic versioning while the project is in its initial public phase.

## Unreleased

### Added

- Added a compact Chinese/English selector beside the DBQuill wordmark. The
  interface locale follows the saved local preference, falls back to the system
  language, and translates static and dynamically rendered application controls
  without sending interface text to a translation service.

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
