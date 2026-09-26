"""Application version + deployment identity (Git SHA) for status, crash reports and Discord.

The Git SHA comes from the environment (`AUREON_GIT_SHA`, set by the Docker build / Jenkins)
or, in a source checkout, from `git rev-parse HEAD`. Never from anything secret.
"""
from __future__ import annotations

import os
import subprocess
from functools import lru_cache
from pathlib import Path

APP_NAME = "aureon-mcx"
APP_VERSION = "0.2.0"


@lru_cache(maxsize=1)
def git_sha() -> str | None:
    env = os.environ.get("AUREON_GIT_SHA", "").strip()
    if env:
        return env
    try:
        root = Path(__file__).resolve().parents[1]
        out = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5)
        sha = out.stdout.strip()
        return sha or None
    except Exception:  # noqa: BLE001 - no git in the container
        return None


def short_sha() -> str:
    sha = git_sha()
    return sha[:7] if sha else "unknown"


def version_info() -> dict:
    return {"app": APP_VERSION, "name": APP_NAME, "git_sha": git_sha(), "git_short": short_sha(),
            "build": os.environ.get("AUREON_BUILD_NUMBER") or None, "branch": os.environ.get("AUREON_GIT_BRANCH") or None}
