from __future__ import annotations

from typing import Any


async def assert_sspanel_module_conformance(client: Any, api_key: str) -> None:
    """Run protocol checks that a compatible SS-panel module must satisfy."""
    missing_auth = await client.get("/module/v1/manifest")
    assert missing_auth.status_code == 401

    health = await client.get("/module/v1/health")
    assert health.status_code == 200
    assert health.json()["status"] in {"ok", "degraded", "unavailable"}

    headers = {"X-SSPanel-Executor-Key": api_key}
    manifest_response = await client.get("/module/v1/manifest", headers=headers)
    assert manifest_response.status_code == 200
    manifest = manifest_response.json()
    assert manifest["moduleId"]
    assert "instagram" == manifest["platform"]
    assert manifest["contractVersions"]
    assert manifest["capabilities"]
    assert manifest["supportsPolling"] is True

    unsupported = await client.post(
        "/module/v1/jobs",
        headers=headers,
        json={
            "idempotencyKey": "conformance:unsupported:v1",
            "orderId": 1,
            "platform": "instagram",
            "actionType": "instagram.dm.send",
            "quantity": 1,
            "accountSelector": {"mode": "system"},
            "payload": {},
        },
    )
    assert unsupported.status_code == 422

    request = {
        "idempotencyKey": "conformance:idempotency:v1",
        "orderId": 1,
        "platform": "instagram",
        "actionType": "instagram.account.health",
        "quantity": 1,
        "accountSelector": {"mode": "system"},
        "payload": {},
    }
    first = await client.post("/module/v1/jobs", headers=headers, json=request)
    second = await client.post("/module/v1/jobs", headers=headers, json=request)
    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["jobId"] == second.json()["jobId"]

    progress = await client.get(
        f"/module/v1/jobs/{first.json()['jobId']}",
        headers=headers,
    )
    assert progress.status_code == 200
    body = progress.json()
    assert body["jobId"] == first.json()["jobId"]
    assert body["status"] in {
        "queued",
        "running",
        "paused",
        "completed",
        "partially_completed",
        "failed",
        "cancelled",
        "challenge_required",
        "rate_limited",
        "session_expired",
        "banned_or_locked",
    }
    assert isinstance(body["completedCount"], int)
    assert isinstance(body["totalCount"], int)
