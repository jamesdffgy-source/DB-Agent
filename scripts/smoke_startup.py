#!/usr/bin/env python3
"""Start the local bridge, probe authenticated HTTP routes, and stop it."""

from __future__ import annotations

import json
import os
import secrets
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = ROOT / "runtime/app"
BRIDGE = APP_ROOT / "frontends/desktop_bridge.py"


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _request(url: str, token: str) -> bytes:
    request = urllib.request.Request(url, headers={"X-DBQuill-Token": token})
    with urllib.request.urlopen(request, timeout=3) as response:
        if response.status != 200:
            raise RuntimeError(f"unexpected HTTP status: {response.status}")
        return response.read()


def main() -> int:
    port = _free_loopback_port()
    token = secrets.token_urlsafe(32)
    env = dict(os.environ)
    env.update(
        {
            "BRIDGE_HOST": "127.0.0.1",
            "BRIDGE_PORT": str(port),
            "BRIDGE_TOKEN": token,
            "PYTHONIOENCODING": "utf-8",
        }
    )
    process = subprocess.Popen(
        [sys.executable, str(BRIDGE)],
        cwd=APP_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    try:
        deadline = time.monotonic() + 40
        status: dict | None = None
        last_error = "bridge did not answer"
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break
            try:
                status = json.loads(
                    _request(f"http://127.0.0.1:{port}/status", token).decode("utf-8")
                )
                break
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                time.sleep(0.25)

        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=2)
            raise RuntimeError(
                f"bridge exited with {process.returncode}\nstdout={stdout[-2000:]}\n"
                f"stderr={stderr[-4000:]}"
            )
        if not status:
            raise RuntimeError(last_error)
        if status.get("ok") is not True or status.get("authRequired") is not True:
            raise RuntimeError(f"invalid status contract: {status}")
        if Path(str(status.get("appRoot", ""))).resolve() != APP_ROOT.resolve():
            raise RuntimeError(f"bridge loaded the wrong app root: {status.get('appRoot')}")

        html = _request(f"http://127.0.0.1:{port}/db", token).decode("utf-8")
        if "DBQuill" not in html:
            raise RuntimeError("desktop route did not return the DBQuill interface")

        print(
            json.dumps(
                {
                    "ok": True,
                    "host": "127.0.0.1",
                    "authenticated_status": True,
                    "desktop_route": True,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
