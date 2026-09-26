"""Deployment artefacts: the Dockerfile exposes 1250 with persistent volumes, a non-root user and
a health check; the Jenkinsfile runs tests before building, health-checks after starting and
never embeds secrets; the webhook notifier renders success / failure without leaking anything."""
from __future__ import annotations

import json

from aureon_mcx.tools.deploy_notify import build_message, main
from tests.conftest import ROOT


def test_dockerfile_contract():
    text = (ROOT / "Dockerfile").read_text()
    assert "EXPOSE 1250" in text and "HEALTHCHECK" in text and "/api/v1/health" in text
    assert 'VOLUME ["/data/sqlite", "/data/parquet", "/data/logs"]' in text
    assert "USER aureon" in text and "tini" in text and 'CMD ["python", "main_aureon.py"]' in text
    assert "AUREON_LOCAL_DB_PATH=/data/sqlite/aureon_mcx.db" in text and "AUREON_PARQUET_DIR=/data/parquet" in text
    for secret in ("DHAN_CLIENT_ID=", "DHAN_ACCESS_TOKEN=", "DISCORD_TOKEN="):
        assert secret not in text  # secrets are never baked into the image
    ignore = (ROOT / ".dockerignore").read_text()
    assert ".env" in ignore and "data/" in ignore and ".git" in ignore


def test_jenkinsfile_contract():
    text = (ROOT / "Jenkinsfile").read_text()
    for stage in ("Checkout", "Resolve Git SHA", "Python syntax / static sanity", "Install dependencies", "Run pytest", "Build Docker image",
                  "Stop / replace previous container", "Start new container", "Health check", "Deployment notification"):
        assert f"stage('{stage}')" in text, stage
    assert text.index("stage('Run pytest')") < text.index("stage('Build Docker image')")  # tests gate the build
    assert "aureon-beast-india" in text and "-p ${API_PORT}:1250" in text and "API_PORT        = '1250'" in text
    assert "${IMAGE_NAME}:${env.GIT_SHA}" in text or 'IMAGE_TAG = "${env.IMAGE_NAME}:${env.GIT_SHA}"' in text
    assert "/api/v1/health" in text and "seq 1 30" in text and "exit 1" in text  # retrying health check that fails the build
    assert "withCredentials" in text and "aureon-dhan-access-token" in text and "refusing to deploy" in text
    assert "${DATA_ROOT}/sqlite:/data/sqlite" in text and "${DATA_ROOT}/parquet:/data/parquet" in text and "DATA_ROOT       = '/data/aureon'" in text
    assert ":rollback" in text and "deploy_notify" in text
    for leak in ("eyJ", "tok-", "password"):
        assert leak not in text


def test_deploy_notify_messages_and_cli(tmp_path, capsys, monkeypatch):
    ok = build_message("success", "abc1234", "main", "0.2.0", "1250", "passed", {"alive": True, "ready": True}, None)
    desc = ok["embeds"][0]["description"]
    assert ok["embeds"][0]["title"] == "✅ Aureon deployed" and "Git: abc1234" in desc and "Branch: main" in desc
    assert "Tests: passed" in desc and "API: healthy (ready: yes)" in desc and "Port: 1250" in desc and "Version: 0.2.0" in desc
    bad = build_message("failure", "abc1234", "main", "0.2.0", "1250", "failed", None, "Health check")
    assert bad["embeds"][0]["title"] == "❌ Aureon deployment failed" and "Stage: Health check" in bad["embeds"][0]["description"]
    health = tmp_path / "health.json"
    health.write_text(json.dumps({"alive": True, "ready": False}))
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)
    assert main(["--status", "success", "--git", "abc1234", "--health", str(health)]) == 0
    assert "notification skipped" in capsys.readouterr().out
    sent = {}
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.example/webhook/secret-path")
    monkeypatch.setattr("aureon_mcx.tools.deploy_notify.send", lambda url, payload, timeout=10.0: sent.update(url=url, payload=payload) or True)
    assert main(["--status", "failure", "--stage", "Run pytest"]) == 0
    out = capsys.readouterr().out
    assert "sent" in out and "secret-path" not in out and sent["payload"]["embeds"][0]["title"].startswith("❌")
