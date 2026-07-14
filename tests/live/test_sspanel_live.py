from __future__ import annotations

import asyncio
import os

import httpx
import pytest

pytestmark = pytest.mark.live


def _live_config() -> tuple[str, dict[str, str], str]:
    if os.getenv("ALLOW_INSTAGRAM_EXECUTOR_LIVE", "false").lower() != "true":
        pytest.skip("Set ALLOW_INSTAGRAM_EXECUTOR_LIVE=true to run live SS-panel executor tests")
    base_url = os.getenv("INSTAGRAM_EXECUTOR_URL", "").rstrip("/")
    api_key = os.getenv("INSTAGRAM_EXECUTOR_API_KEY", "")
    account_id = os.getenv("INSTAGRAM_LIVE_EXECUTOR_ACCOUNT_ID", "")
    if not base_url or not api_key or not account_id:
        pytest.skip("INSTAGRAM_EXECUTOR_URL, INSTAGRAM_EXECUTOR_API_KEY and INSTAGRAM_LIVE_EXECUTOR_ACCOUNT_ID are required")
    return base_url, {"X-SSPanel-Executor-Key": api_key}, account_id


@pytest.mark.asyncio
async def test_live_sspanel_account_health_contract():
    base_url, headers, account_id = _live_config()
    async with httpx.AsyncClient(base_url=base_url, headers=headers, timeout=30.0) as client:
        manifest = await client.get("/module/v1/manifest")
        assert manifest.status_code == 200
        assert "instagram.account.health" in manifest.json()["capabilities"]

        request = {
            "idempotencyKey": "live:instagram:account-health:v1",
            "orderId": 1,
            "platform": "instagram",
            "actionType": "instagram.account.health",
            "quantity": 1,
            "accountSelector": {"mode": "specific", "accountIds": [account_id]},
            "payload": {},
        }
        first = await client.post("/module/v1/jobs", json=request)
        second = await client.post("/module/v1/jobs", json=request)
        assert first.status_code == second.status_code == 202
        assert first.json()["jobId"] == second.json()["jobId"]

        job_id = first.json()["jobId"]
        for _ in range(20):
            progress = await client.get(f"/module/v1/jobs/{job_id}")
            assert progress.status_code == 200
            if progress.json()["status"] in {"completed", "failed", "challenge_required", "session_expired", "banned_or_locked"}:
                break
            await asyncio.sleep(1)
        assert progress.json()["status"] == "completed", progress.json()
