import logging

import pytest
from httpx import ASGITransport, AsyncClient

from aiograpi_rest.main import app
from aiograpi_rest.routers import sspanel


@pytest.fixture(autouse=True)
def sspanel_env(monkeypatch, tmp_path):
    monkeypatch.setenv("SSPANEL_EXECUTOR_API_KEY", "executor-secret")
    monkeypatch.setenv("SSPANEL_EXECUTOR_DB_PATH", str(tmp_path / "sspanel-jobs.json"))
    sspanel.reset_store_for_tests()
    yield
    sspanel.reset_store_for_tests()


async def _post_job(
    client,
    *,
    idempotency_key="instagram:order:123:v1",
    action_type="instagram.account.health",
    account_selector=None,
):
    return await client.post(
        "/sspanel/jobs",
        headers={"X-SSPanel-Executor-Key": "executor-secret"},
        json={
            "idempotencyKey": idempotency_key,
            "orderId": 123,
            "platform": "instagram",
            "actionType": action_type,
            "quantity": 1,
            "accountSelector": account_selector or {"mode": "system"},
            "payload": {},
        },
    )


async def _import_session(client):
    return await client.post(
        "/sspanel/accounts/import-session",
        headers={"X-SSPanel-Executor-Key": "executor-secret"},
        json={
            "username": "ig_user",
            "sessionid": "raw-session-secret",
            "proxy": "http://proxy-user:proxy-pass@example.test:8000",
            "settings": {"authorization_data": {"sessionid": "nested-session-secret"}},
        },
    )


class FakeHealthClient:
    def __init__(self, account_info=None, failure=None):
        self.account_info_response = account_info or {"pk": 123, "username": "ig_user"}
        self.failure = failure
        self.calls = []

    async def account_info(self):
        self.calls.append("account_info")
        if self.failure:
            raise self.failure
        return self.account_info_response

    async def get_timeline_feed(self):
        self.calls.append("get_timeline_feed")
        if self.failure:
            raise self.failure
        return {"items": []}


@pytest.mark.asyncio
async def test_sspanel_facade_rejects_missing_or_invalid_api_key():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        missing = await client.get("/sspanel/jobs/job_1")
        invalid = await client.get("/sspanel/jobs/job_1", headers={"X-SSPanel-Executor-Key": "wrong"})

    assert missing.status_code == 401
    assert invalid.status_code == 401


@pytest.mark.asyncio
async def test_sspanel_jobs_are_idempotent_by_idempotency_key():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        first = await _post_job(client)
        second = await _post_job(client)

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["jobId"] == second.json()["jobId"]
    assert first.json()["status"] == "queued"


@pytest.mark.asyncio
async def test_sspanel_job_status_is_toolkit_compatible():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await _post_job(client)
        job_id = created.json()["jobId"]
        status = await client.get(f"/sspanel/jobs/{job_id}", headers={"X-SSPanel-Executor-Key": "executor-secret"})

    assert status.status_code == 200
    assert status.json() == {
        "jobId": job_id,
        "status": "queued",
        "completedCount": 0,
        "totalCount": 1,
        "result": {},
        "accountHealth": {},
    }


@pytest.mark.asyncio
async def test_sspanel_account_health_job_completes_for_imported_specific_account(monkeypatch):
    fake_client = FakeHealthClient()
    factory_accounts = []

    def fake_factory(account):
        factory_accounts.append(account)
        return fake_client

    monkeypatch.setattr(sspanel, "create_instagram_health_client", fake_factory, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        imported = await _import_session(client)
        account_id = imported.json()["executorAccountId"]
        created = await _post_job(
            client,
            account_selector={"mode": "specific", "accountIds": [account_id]},
        )
        job_id = created.json()["jobId"]
        status = await client.get(f"/sspanel/jobs/{job_id}", headers={"X-SSPanel-Executor-Key": "executor-secret"})

    assert created.status_code == 202
    assert created.json()["status"] == "completed"
    assert status.json()["status"] == "completed"
    assert status.json()["completedCount"] == 1
    assert status.json()["totalCount"] == 1
    assert status.json()["result"] == {"ok": True}
    assert status.json()["accountHealth"] == {
        "executorAccountId": account_id,
        "status": "healthy",
        "username": "ig_user",
    }
    assert fake_client.calls == ["account_info"]
    assert factory_accounts[0]["executorAccountId"] == account_id
    assert factory_accounts[0]["sessionid"] == "raw-session-secret"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("message", "expected_status", "expected_code"),
    [
        ("challenge_required: checkpoint required", "challenge_required", "challenge_required"),
        ("login_required: session expired", "session_expired", "session_expired"),
        ("user is banned or locked", "banned_or_locked", "banned_or_locked"),
    ],
)
async def test_sspanel_account_health_job_maps_terminal_instagram_failures(
    monkeypatch,
    message,
    expected_status,
    expected_code,
):
    monkeypatch.setattr(
        sspanel,
        "create_instagram_health_client",
        lambda _account: FakeHealthClient(failure=RuntimeError(message)),
        raising=False,
    )

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        imported = await _import_session(client)
        account_id = imported.json()["executorAccountId"]
        created = await _post_job(
            client,
            idempotency_key=f"instagram:order:{expected_code}:v1",
            account_selector={"mode": "specific", "accountIds": [account_id]},
        )
        job_id = created.json()["jobId"]
        status = await client.get(f"/sspanel/jobs/{job_id}", headers={"X-SSPanel-Executor-Key": "executor-secret"})

    body = status.json()
    assert created.status_code == 202
    assert created.json()["status"] == expected_status
    assert body["status"] == expected_status
    assert body["completedCount"] == 0
    assert body["totalCount"] == 1
    assert body["errorCode"] == expected_code
    assert message in body["errorMessage"]


@pytest.mark.asyncio
async def test_sspanel_jobs_reject_unsupported_actions():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await _post_job(client, action_type="instagram.mass.follow")

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"][-1] == "actionType"


@pytest.mark.asyncio
async def test_sspanel_account_import_does_not_log_sensitive_values(caplog):
    caplog.set_level(logging.INFO)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/sspanel/accounts/import-session",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
            json={
                "username": "ig_user",
                "sessionid": "raw-session-secret",
                "password": "raw-password",
                "proxy": "http://proxy-user:proxy-pass@example.test:8000",
                "settings": {"authorization_data": {"sessionid": "nested-session-secret"}},
            },
        )

    logs = caplog.text
    assert response.status_code == 200
    assert response.json()["executorAccountId"].startswith("ig_acc_")
    assert "raw-session-secret" not in logs
    assert "raw-password" not in logs
    assert "proxy-pass" not in logs
    assert "nested-session-secret" not in logs
