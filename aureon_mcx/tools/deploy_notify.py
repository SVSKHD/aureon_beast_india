"""Deployment notification to Discord through a webhook (Jenkins post-deploy step).

    python -m aureon_mcx.tools.deploy_notify --status success --git abc1234 --branch main \
        --version 0.2.0 --port 1250 --health health.json --tests passed

The webhook URL comes from the DISCORD_WEBHOOK_URL environment variable (a Jenkins secret);
nothing secret is ever printed. Exit code 0 even when Discord is unreachable (a notification
must never fail a deployment that already passed its health check).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


def build_message(status: str, git: str, branch: str, version: str, port: str, tests: str, health: dict | None, stage: str | None) -> dict:
    if status == "success":
        api = "healthy" if health and health.get("alive") else "unknown"
        ready = health.get("ready") if health else None
        lines = ["✅ Aureon deployed", f"Git: {git}", f"Branch: {branch}", f"Tests: {tests}",
                 f"API: {api}" + (f" (ready: {'yes' if ready else 'not yet'})" if health else ""), f"Port: {port}", f"Version: {version}"]
        color = 0x2ECC71
    else:
        lines = ["❌ Aureon deployment failed", f"Stage: {stage or 'unknown'}", f"Git: {git}", f"Branch: {branch}", f"Version: {version}"]
        color = 0xE74C3C
    return {"embeds": [{"title": lines[0], "description": "\n".join(lines[1:]), "color": color}]}


def send(webhook_url: str, payload: dict, timeout: float = 10.0) -> bool:
    req = urllib.request.Request(webhook_url, data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - operator-configured webhook
            return 200 <= resp.status < 300
    except (urllib.error.URLError, OSError) as exc:
        print(f"deploy_notify: webhook delivery failed ({type(exc).__name__})", file=sys.stderr)
        return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Send an Aureon deployment notification to Discord")
    parser.add_argument("--status", choices=["success", "failure"], required=True)
    parser.add_argument("--git", default="unknown")
    parser.add_argument("--branch", default="unknown")
    parser.add_argument("--version", default="unknown")
    parser.add_argument("--port", default="1250")
    parser.add_argument("--tests", default="passed")
    parser.add_argument("--health", default=None, help="path to the health JSON captured by the pipeline")
    parser.add_argument("--stage", default=None)
    args = parser.parse_args(argv)
    health = None
    if args.health and os.path.exists(args.health):
        try:
            with open(args.health, encoding="utf-8") as fh:
                health = json.load(fh)
        except (OSError, ValueError):
            health = None
    payload = build_message(args.status, args.git, args.branch, args.version, args.port, args.tests, health, args.stage)
    url = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        print("deploy_notify: DISCORD_WEBHOOK_URL not set; notification skipped")
        return 0
    ok = send(url, payload)
    print("deploy_notify: sent" if ok else "deploy_notify: not delivered")
    return 0


if __name__ == "__main__":
    sys.exit(main())
