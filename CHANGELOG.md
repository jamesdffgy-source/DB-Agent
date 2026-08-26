# Changelog

All notable user-facing changes are recorded here. Versions follow semantic versioning while the project is in its initial public phase.

## 0.1.0 - 2026-08-26

### Added

- Natural-language database operation planning with a typed relational contract.
- Complete SQLite workflow for schema exploration, retrieval, charts, semantic definitions, scheduling, and confirmed writes.
- Read-only MySQL 8.4 and PostgreSQL 17 paths with schema discovery, bounded execution, and timeout handling.
- Table-aware insert forms, change previews, one-time confirmation, local authorization, and append-only audit records.
- CSV and `.xlsx` import, session management, chart caching, and local model-profile settings.
- Hash-locked Windows installation, clean-machine diagnostics, CI gate, and authenticated startup smoke test.

### Known limits

- Distribution is source-first and requires Windows x64, Python 3.12, and WebView2.
- MySQL and PostgreSQL writes are not supported.
- macOS, Linux, Windows on ARM, and legacy `.xls` imports are not verified release targets.
