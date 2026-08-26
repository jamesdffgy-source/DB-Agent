## What changed

Describe the user-visible problem and the smallest implemented change.

## Evidence

- [ ] Added or updated focused regression coverage.
- [ ] Ran `scripts\check_project.cmd`.
- [ ] Used only synthetic or anonymized test data.
- [ ] Updated documentation when behavior, dependencies, or supported scope changed.

Paste the relevant test summary without credentials, private SQL, database rows, or model keys.

## Safety review

- [ ] Query behavior remains read-only by default.
- [ ] Writes still require validation, preview, and explicit confirmation.
- [ ] Authorization, single-statement validation, row limits, and local API protections are not weakened.
- [ ] New files and dependencies have clear provenance and compatible licenses.

## Compatibility

List the Windows, Python, and database versions exercised by this change.
