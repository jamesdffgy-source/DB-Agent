# -*- coding: utf-8 -*-
"""
DBQuill 桌面前端启动器
双击运行：打开独立桌面窗口承载 DBQuill（http://127.0.0.1:14169/db），无需浏览器。
若没有协议兼容的 bridge，自动在可用端口以隐藏窗口启动；旧版本 bridge 不会被新前端复用。
"""
import os
import sys
import json
import time
import socket
import secrets
import urllib.request
import urllib.parse
import subprocess

DEMO_ROOT = os.path.dirname(os.path.abspath(__file__))
APP_ROOT = os.path.join(DEMO_ROOT, "runtime", "app")
BRIDGE = os.path.join(APP_ROOT, "frontends", "desktop_bridge.py")
APP_ICON = os.path.join(APP_ROOT, "frontends", "desktop", "static", "dbquill-icon-v2.ico")
PREFERRED_BRIDGE_PORT = 14169
FALLBACK_BRIDGE_PORTS = range(14170, 14180)
TOKEN_FILE = os.path.join(APP_ROOT, "temp", "bridge.token")
EXPECTED_BRIDGE_PROTOCOL = 2
EXPECTED_UPLOAD_PROTOCOL = "multipart-v1"


def _select_python():
    """Select the same Python 3.12 runtime for the launcher and bridge."""
    candidates = []
    configured = (
        os.environ.get("DBQUILL_PYTHON", "").strip()
        or os.environ.get("DBAGENT_PYTHON", "").strip()
    )
    if configured:
        candidates.append(configured)
    candidates.extend((
        os.path.join(DEMO_ROOT, "runtime", "python", "python.exe"),
        os.path.join(DEMO_ROOT, ".venv", "Scripts", "python.exe"),
        sys.executable,
    ))
    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            return os.path.abspath(candidate)
    raise RuntimeError(
        "未找到可用的 Python。请先运行 scripts\\bootstrap_dev.cmd。"
    )


PYTHON = _select_python()


def _load_bridge_token():
    """Load the persistent loopback API token, creating it atomically if needed."""
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    try:
        with open(TOKEN_FILE, "x", encoding="ascii") as f:
            token = secrets.token_urlsafe(32)
            f.write(token)
            return token
    except FileExistsError:
        with open(TOKEN_FILE, "r", encoding="ascii") as f:
            token = f.read().strip()
        if token:
            return token
        token = secrets.token_urlsafe(32)
        with open(TOKEN_FILE, "w", encoding="ascii") as f:
            f.write(token)
        return token


BRIDGE_TOKEN = _load_bridge_token()


def _base_url(port):
    return f"http://127.0.0.1:{port}"


def _status_info(port):
    try:
        req = urllib.request.Request(
            f"{_base_url(port)}/status",
            headers={"X-DBQuill-Token": BRIDGE_TOKEN},
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def _status_ok(port):
    st = _status_info(port) or {}
    return (
        bool(st.get("ok"))
        and st.get("authRequired") is True
        and st.get("bridgeProtocol") == EXPECTED_BRIDGE_PROTOCOL
        and st.get("uploadProtocol") == EXPECTED_UPLOAD_PROTOCOL
        and os.path.normcase(os.path.normpath(st.get("appRoot", ""))) == os.path.normcase(APP_ROOT)
    )


def _port_occupied(port):
    s = socket.socket()
    s.settimeout(1)
    try:
        s.connect(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def _select_bridge_port():
    """Reuse this project's bridge or choose a free loopback fallback port."""
    for port in (PREFERRED_BRIDGE_PORT, *FALLBACK_BRIDGE_PORTS):
        if _status_ok(port):
            return port, True
    for port in (PREFERRED_BRIDGE_PORT, *FALLBACK_BRIDGE_PORTS):
        if not _port_occupied(port):
            return port, False
    raise RuntimeError("DBQuill 可用端口已占满（14169–14179）。")


def ensure_bridge():
    bridge_port, already_running = _select_bridge_port()
    if already_running:
        return bridge_port
    log_dir = os.path.join(APP_ROOT, "temp")
    os.makedirs(log_dir, exist_ok=True)
    env = dict(os.environ)
    env["BRIDGE_PORT"] = str(bridge_port)
    env["BRIDGE_TOKEN"] = BRIDGE_TOKEN
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    with open(os.path.join(log_dir, "demo-bridge.stdout.log"), "ab") as so, \
         open(os.path.join(log_dir, "demo-bridge.stderr.log"), "ab") as se:
        subprocess.Popen([PYTHON, BRIDGE], cwd=APP_ROOT, env=env,
                         stdout=so, stderr=se, creationflags=flags)
    deadline = time.time() + 60
    while time.time() < deadline:
        if _status_ok(bridge_port):
            return bridge_port
        time.sleep(0.5)
    raise RuntimeError(
        f"bridge 在端口 {bridge_port} 上 60 秒内未就绪，请查看 "
        "runtime/app/temp/demo-bridge.stderr.log"
    )


def main():
    bridge_port = ensure_bridge()
    db_url = (
        f"{_base_url(bridge_port)}/db?token="
        f"{urllib.parse.quote(BRIDGE_TOKEN, safe='')}"
    )
    import webview  # 延迟导入：先确保 bridge 就绪再拉起 GUI

    w, h = 1240, 820
    try:
        import ctypes
        sw = ctypes.windll.user32.GetSystemMetrics(0)
        sh = ctypes.windll.user32.GetSystemMetrics(1)
        x = max(0, (sw - w) // 2)
        y = max(0, (sh - h) // 2)
    except Exception:
        x, y = None, None
    win = webview.create_window(
        "DBQuill · Natural-Language Database Agent", db_url,
        width=w, height=h, x=x, y=y,
        resizable=True, text_select=True, min_size=(900, 600),
    )
    webview.start(icon=APP_ICON if os.path.isfile(APP_ICON) else None)


if __name__ == "__main__":
    main()
