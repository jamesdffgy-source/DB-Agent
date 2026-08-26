# Installation

DB-Agent currently ships as a source-first Windows desktop application. It does not yet provide a signed native installer.

## Supported environment

| Component | Requirement |
| --- | --- |
| Operating system | Windows 10 or Windows 11, x64 |
| Python | CPython 3.12; CI selects an available 3.12 patch release |
| Web renderer | Microsoft Edge WebView2 Runtime |
| Network | Required once to clone and install locked Python packages; required later only for the configured model endpoint |
| Databases | SQLite; CSV/`.xlsx` import; read-only MySQL 8.4 and PostgreSQL 17 paths |

The source workflow has been validated in a fresh export and in GitHub-hosted Windows CI. Other Python versions, Windows on ARM, macOS, and Linux are not release targets yet.

## Recommended installation

Open PowerShell or Command Prompt:

```powershell
git clone https://github.com/jamesdffgy-source/DB-Agent.git
cd DB-Agent
.\scripts\install_and_start.cmd
```

The installer script performs three local steps:

1. creates `.venv` with Python 3.12;
2. installs `requirements.lock` with package hashes enforced and runs `pip check`;
3. verifies imports, writable local state, and WebView2 detection before starting the app.

No database or model credential is required to install the application.

## Manual installation

```powershell
.\scripts\bootstrap_dev.cmd
.\scripts\doctor.cmd
.\scripts\start_dbagent.cmd
```

If Python discovery is ambiguous, select a specific Python 3.12 executable for the current terminal:

```powershell
$env:DBAGENT_PYTHON = 'C:\Path\To\Python312\python.exe'
.\scripts\bootstrap_dev.cmd
```

## First run

1. Open **Settings** and create a model profile for an OpenAI-compatible text endpoint.
2. Use **Add database** to attach a SQLite file, import CSV/`.xlsx`, or create a read-only MySQL/PostgreSQL connection.
3. Use synthetic data for the first test. The included demo is created with:

```powershell
.\scripts\run_python.cmd scripts\create_demo_database.py
```

Local model profiles are stored at `runtime/app/model_profiles.json`. Runtime databases, tokens, uploads, sessions, audit state, and logs are also local and ignored by Git.

## Verify the installation

```powershell
.\scripts\doctor.cmd
.\scripts\run_python.cmd scripts\smoke_startup.py
.\scripts\check_project.cmd
```

The smoke test starts the authenticated loopback service on a temporary port, requests `/status` and the desktop route, and shuts the process down. It does not connect to a user database or model provider.

## Troubleshooting

### Python 3.12 was not found

Install CPython 3.12 x64, enable the Python launcher during installation, reopen the terminal, and run `py -3.12 --version`.

### The desktop window does not open

Install or repair the [Microsoft Edge WebView2 Evergreen Runtime](https://developer.microsoft.com/microsoft-edge/webview2/). Then run `scripts\doctor.cmd` again. The local bridge log is written under `runtime/app/temp/`.

### Package installation is slow

The bootstrap stores downloads in the repository-local `.cache/pip` directory. If a proxy is enabled on the device, verify that it is reachable and that Python package downloads are allowed.

### Offline installation

The committed lock file makes versions and hashes reproducible, but the repository does not vendor Python wheels. Prepare an approved wheelhouse on a connected Windows x64 device, verify every artifact against the lock file, and install from that wheelhouse before moving the source tree offline.

## Uninstall

Stop the desktop app, then remove the cloned directory. DB-Agent does not install a Windows service or write application credentials into the repository. Back up any local databases or audit exports you want to retain before removal.
