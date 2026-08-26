# Contributing to DBQuill

DBQuill treats natural-language database work as a safety-sensitive product. Changes should preserve the product boundary and architecture decisions documented under `docs/`.

## Before you start

- Search existing issues and keep one change per pull request.
- Use the issue forms for defects and feature proposals.
- Never use a production database or private record in a reproduction.
- Discuss large architecture or compatibility changes in an issue before implementation.

By participating, you agree to follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Local setup

The supported source-development environment is Windows x64 with CPython 3.12:

```powershell
.\scripts\bootstrap_dev.cmd
.\scripts\doctor.cmd
```

Start the desktop application and add a local model profile from Settings. The generated `runtime/app/model_profiles.json` is ignored and must never be committed.

## Verification

Run the completion gate before opening a pull request:

```powershell
.\scripts\check_project.cmd
```

If code, configuration, resources, dependencies, or architecture changed, update the project records and record the new verified fingerprint:

```powershell
.\scripts\check_project.cmd --record --summary "concise change summary"
```

Never commit local credentials, uploaded databases, generated benchmark output, bridge tokens, audit/session state, logs, or the portable `runtime/python/` directory. Query behavior must remain read-only by default; writes require validation, preview, and explicit confirmation.

Also run the authenticated startup probe when changing dependencies, startup, routing, or packaging:

```powershell
.\scripts\run_python.cmd scripts\smoke_startup.py
```

## Pull requests

Create a focused branch, add regression coverage for behavioral changes, and explain the safety impact in the pull request template. A reviewable pull request should contain:

- the user-visible problem and intended behavior;
- the smallest implementation that solves it;
- tests using synthetic data;
- documentation for changed behavior or supported scope;
- a passing project gate with no credentials or private output.

Keep generated dependencies, benchmark downloads, local environments, and runtime state out of commits. Do not weaken authorization, CORS, single-statement validation, row limits, physical read-only connections, or the write-preview confirmation boundary.

## Benchmarks

Use benchmark results as diagnostic evidence, not as a template for hard-coded fixes. Keep infrastructure failures out of the accuracy denominator, retain raw run metadata locally, and document the model/configuration, dataset subset, scorer version, and sample size for every reported number.
