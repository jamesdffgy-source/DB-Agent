# Security Policy

DBQuill can inspect and modify databases, so security reports are handled as product-safety issues rather than ordinary bugs.

## Supported versions

The latest tagged release and the current `main` branch receive security fixes. Older initial-development snapshots are not supported.

## Reporting a vulnerability

Do not open a public issue for a suspected authentication bypass, SQL safety bypass, write-confirmation bypass, credential exposure, scope escape, file access issue, or audit-integrity weakness. Open the repository's **Security** tab and choose **Report a vulnerability** to create a private report. If that control is unavailable, contact the repository owner privately without sending exploit data in a public channel.

Include only the information needed to reproduce the issue:

- affected commit and environment;
- expected and observed behavior;
- a minimal reproduction using synthetic data;
- the security impact and any known preconditions.

Do not send live API keys, database credentials, private database files, production SQL, audit databases, session stores, or query results. Maintainers will acknowledge a complete report as soon as practical, reproduce it with synthetic data, and coordinate remediation and disclosure in the private advisory. No response-time or bounty guarantee is currently offered.

For non-sensitive defects and feature requests, use the regular GitHub issue tracker.
