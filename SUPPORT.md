# Support

## Installation and usage

Read the [installation guide](docs/INSTALLATION.md), run `scripts\doctor.cmd`, and search existing issues before opening a new one. A useful report includes:

- the DB-Agent release or commit;
- Windows and Python versions;
- the exact step that failed;
- sanitized diagnostic output;
- a minimal reproduction using synthetic data.

Do not attach credentials, private databases, production SQL, raw audit stores, or model keys.

## Bugs and feature requests

Use the structured GitHub issue forms. Keep one problem or proposal per issue and describe the database engine and safety impact where relevant.

## Security reports

Do not open a public issue for a suspected authentication, authorization, SQL safety, write-confirmation, credential, file-access, or audit-integrity weakness. Follow [SECURITY.md](SECURITY.md) instead.

## Support boundary

The current release target is Windows x64 with Python 3.12. SQLite is the complete product path; MySQL and PostgreSQL are verified read-only paths. Requests outside that matrix are welcome as proposals but are not treated as confirmed defects until the target is supported.
