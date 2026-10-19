#!/usr/bin/env python3
"""Install this project's supervisor as a per-user macOS LaunchAgent."""
from pathlib import Path
import json
import os
import plistlib
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "environments/attention_judgment/runs/local-pipeline"
LABEL = "local.attention-proxy"


def main():
    if sys.platform != "darwin":
        raise SystemExit("This installer is for macOS; use local_pipeline.py start on other platforms.")
    target = Path.home() / "Library/LaunchAgents" / f"{LABEL}.plist"
    command = [str(ROOT / ".venv/bin/python"), str(ROOT / "scripts/local_pipeline.py"), "serve"]
    if target.exists() and plistlib.loads(target.read_bytes()).get("ProgramArguments") != command:
        raise SystemExit("An unrelated service already uses this label; no changes made.")
    domain = f"gui/{os.getuid()}"
    installed = subprocess.run(["launchctl", "print", f"{domain}/{LABEL}"], capture_output=True).returncode == 0
    if installed:
        subprocess.run(["launchctl", "bootout", f"{domain}/{LABEL}"], check=True)
    else:
        state = RUNTIME / "supervisor.json"
        if state.exists():
            pid = json.loads(state.read_text())["pid"]
            process = subprocess.run(["ps", "-p", str(pid), "-o", "args="], capture_output=True, text=True)
            if str(ROOT / "scripts/local_pipeline.py") in process.stdout and " serve" in process.stdout:
                os.kill(pid, signal.SIGTERM)
                for _ in range(50):
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.2)
    target.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME.mkdir(parents=True, exist_ok=True)
    document = {"Label": LABEL, "ProgramArguments": command,
                "WorkingDirectory": str(ROOT), "RunAtLoad": True, "KeepAlive": True,
                "ThrottleInterval": 30,
                "EnvironmentVariables": {"PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
                                         "PYTHONUNBUFFERED": "1"},
                "StandardOutPath": str(RUNTIME / "supervisor.log"),
                "StandardErrorPath": str(RUNTIME / "supervisor.log")}
    target.write_bytes(plistlib.dumps(document))
    target.chmod(0o600)
    subprocess.run(["launchctl", "bootstrap", domain, str(target)], check=True)
    print(f"Installed {LABEL}; configuration: {target}")


if __name__ == "__main__":
    main()
