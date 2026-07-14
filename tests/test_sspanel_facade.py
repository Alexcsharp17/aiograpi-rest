import asyncio
import json
import logging

import pytest
from aiograpi import exceptions as aiograpi_exceptions
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from aiograpi_rest.main import app
from aiograpi_rest.routers import sspanel


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (aiograpi_exceptions.ChallengeRequired(), "challenge_required"),
        (aiograpi_exceptions.CheckpointRequired(), "challenge_required"),
        (aiograpi_exceptions.LoginRequired(), "session_expired"),
        (aiograpi_exceptions.ClientLoginRequired(), "session_expired"),
        (aiograpi_exceptions.AccountSuspended(), "banned_or_locked"),
        (aiograpi_exceptions.FeedbackRequired(), "rate_limited"),
        (aiograpi_exceptions.PleaseWaitFewMinutes(), "rate_limited"),
        (aiograpi_exceptions.RateLimitError(), "rate_limited"),
    ],
)
def test_typed_aiograpi_exceptions_map_to_terminal_external_status(error, expected):
    assert sspanel._exception_status(error) == expected


@pytest.fixture(autouse=True)
def sspanel_env(monkeypatch, tmp_path):
    monkeypatch.setenv("SSPANEL_EXECUTOR_API_KEY", "executor-secret")
    monkeypatch.setenv(
        "SSPANEL_EXECUTOR_ENCRYPTION_KEY",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
    )
    monkeypatch.setenv("SSPANEL_EXECUTOR_DB_PATH", str(tmp_path / "sspanel-jobs.json"))
    sspanel.reset_store_for_tests()
    yield
    sspanel.reset_store_for_tests()


async def _post_job(
    client,
    *,
    idempotency_key="instagram:order:123:v1",
    action_type="instagram.account.health",
    quantity=1,
    account_selector=None,
    payload=None,
    policy_envelope=None,
):
    return await client.post(
        "/module/v1/jobs",
        headers={"X-SSPanel-Executor-Key": "executor-secret"},
        json={
            "idempotencyKey": idempotency_key,
            "orderId": 123,
            "platform": "instagram",
            "actionType": action_type,
            "quantity": quantity,
            "accountSelector": account_selector or {"mode": "system"},
            "payload": payload or {},
            **({"policyEnvelope": policy_envelope} if policy_envelope else {}),
        },
    )


async def _import_session(client):
    return await client.post(
        "/module/v1/accounts/import-session",
        headers={"X-SSPanel-Executor-Key": "executor-secret"},
        json={
            "username": "ig_user",
            "sessionid": "raw-session-secret",
            "proxy": "http://proxy-user:proxy-pass@example.test:8000",
            "settings": {"authorization_data": {"sessionid": "nested-session-secret"}},
        },
    )


async def _start_specific_job(client, action_type, payload, *, idempotency_key):
    imported = await _import_session(client)
    account_id = imported.json()["executorAccountId"]
    created = await _post_job(
        client,
        idempotency_key=idempotency_key,
        action_type=action_type,
        account_selector={"mode": "specific", "accountIds": [account_id]},
        payload=payload,
    )
    status = await _wait_for_terminal(client, created.json()["jobId"])
    return created, status


async def _wait_for_terminal(client, job_id):
    terminal = {
        "completed",
        "partially_completed",
        "failed",
        "cancelled",
        "challenge_required",
        "rate_limited",
        "session_expired",
        "banned_or_locked",
    }
    for _ in range(100):
        response = await client.get(
            f"/module/v1/jobs/{job_id}",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
        )
        if response.json()["status"] in terminal:
            return response
        await asyncio.sleep(0.01)
    raise AssertionError(f"Job {job_id} did not reach a terminal status")


async def _wait_for_terminal_or_paused(client, job_id):
    terminal_or_paused = {
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
    for _ in range(100):
        response = await client.get(
            f"/module/v1/jobs/{job_id}",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
        )
        if response.json()["status"] in terminal_or_paused:
            return response
        await asyncio.sleep(0.01)
    raise AssertionError(f"Job {job_id} did not reach paused or terminal status")


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

    async def user_info_by_username(self, username):
        self.calls.append(("user_info_by_username", username))
        if self.failure:
            raise self.failure
        return {"pk": 456, "username": username, "full_name": "Profile User"}

    async def user_info(self, user_id):
        self.calls.append(("user_info", user_id))
        if self.failure:
            raise self.failure
        return {"pk": int(user_id), "username": "profile_by_id"}

    async def media_comments_chunk(self, media_id, amount, cursor):
        self.calls.append(("media_comments_chunk", media_id, amount, cursor))
        if self.failure:
            raise self.failure
        return ([{"pk": 11, "text": "Hello"}], "next-page")

    async def media_comment(self, media_id, text, replied_to_comment_id=None):
        self.calls.append(("media_comment", media_id, text, replied_to_comment_id))
        if self.failure:
            raise self.failure
        return {"pk": 22, "text": text, "media_id": media_id}


class FakeCapabilityClient(FakeHealthClient):
    async def photo_upload(self, path, caption, **kwargs):
        self.calls.append(("photo_upload", path.suffix, caption))
        return {"pk": 101, "media_type": 1, "caption": caption}

    async def video_upload(self, path, caption, **kwargs):
        self.calls.append(("video_upload", path.suffix, caption))
        return {"pk": 102, "media_type": 2, "caption": caption}

    async def clip_upload(self, path, caption, **kwargs):
        self.calls.append(("clip_upload", path.suffix, caption))
        return {"pk": 103, "media_type": 2, "product_type": "clips", "caption": caption}

    async def photo_upload_to_story(self, path, caption, **kwargs):
        self.calls.append(("photo_upload_to_story", path.suffix, caption))
        return {"pk": 104, "media_type": 1, "caption": caption}

    async def video_upload_to_story(self, path, caption, **kwargs):
        self.calls.append(("video_upload_to_story", path.suffix, caption))
        return {"pk": 105, "media_type": 2, "caption": caption}

    async def comment_bulk_delete(self, media_id, comment_ids):
        self.calls.append(("comment_bulk_delete", media_id, comment_ids))
        return True

    async def comment_pin(self, media_id, comment_id):
        self.calls.append(("comment_pin", media_id, comment_id))
        return True

    async def direct_threads(self, **kwargs):
        self.calls.append(("direct_threads", kwargs))
        return [{"thread_id": 301, "messages": [{"text": "hello"}]}]

    async def direct_send(self, text, user_ids=None, thread_ids=None):
        self.calls.append(("direct_send", text, user_ids, thread_ids))
        return {"id": "302", "text": text}

    async def direct_answer(self, thread_id, text):
        self.calls.append(("direct_answer", thread_id, text))
        return {"id": "303", "thread_id": thread_id, "text": text}

    async def insights_account(self):
        self.calls.append(("insights_account",))
        return {"reach": 10, "impressions": 20}

    async def insights_media(self, media_id):
        self.calls.append(("insights_media", media_id))
        return {"media_id": media_id, "reach": 5}


@pytest.mark.asyncio
async def test_sspanel_facade_rejects_missing_or_invalid_api_key():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        missing = await client.get("/module/v1/jobs/job_1")
        invalid = await client.get("/module/v1/jobs/job_1", headers={"X-SSPanel-Executor-Key": "wrong"})

    assert missing.status_code == 401
    assert invalid.status_code == 401


@pytest.mark.asyncio
async def test_scoped_credential_ring_supports_rotation_without_widening_scopes(monkeypatch):
    monkeypatch.setenv(
        "SSPANEL_EXECUTOR_API_KEYS",
        json.dumps([
            {"id": "active", "secret": "new-secret", "scopes": ["manifest:read", "jobs:read", "jobs:write", "accounts:read", "accounts:write"]},
            {"id": "previous", "secret": "old-secret", "scopes": ["jobs:read"]},
        ]),
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        old_read = await client.get(
            "/module/v1/jobs/missing",
            headers={"X-SSPanel-Executor-Key": "old-secret", "X-SSPanel-Executor-Key-Id": "previous"},
        )
        old_manifest = await client.get(
            "/module/v1/manifest",
            headers={"X-SSPanel-Executor-Key": "old-secret", "X-SSPanel-Executor-Key-Id": "previous"},
        )
        old_write = await client.post(
            "/module/v1/jobs",
            headers={"X-SSPanel-Executor-Key": "old-secret", "X-SSPanel-Executor-Key-Id": "previous"},
            json={
                "idempotencyKey": "scoped:old-write",
                "orderId": 1,
                "platform": "instagram",
                "actionType": "instagram.account.health",
                "quantity": 1,
                "accountSelector": {"mode": "system"},
                "payload": {},
            },
        )
        new_write = await client.post(
            "/module/v1/jobs",
            headers={"X-SSPanel-Executor-Key": "new-secret", "X-SSPanel-Executor-Key-Id": "active"},
            json={
                "idempotencyKey": "scoped:new-write",
                "orderId": 1,
                "platform": "instagram",
                "actionType": "instagram.account.health",
                "quantity": 1,
                "accountSelector": {"mode": "system"},
                "payload": {},
            },
        )

    assert old_read.status_code == 404
    assert old_manifest.status_code == 403
    assert old_write.status_code == 403
    assert new_write.status_code == 202


@pytest.mark.asyncio
async def test_sspanel_jobs_are_idempotent_by_idempotency_key():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        first = await _post_job(client)
        second = await _post_job(client)

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["jobId"] == second.json()["jobId"]
    assert first.json()["status"] == "queued"


def test_executor_events_are_enqueued_durably_for_configured_callbacks(tmp_path, monkeypatch):
    monkeypatch.setenv("SSPANEL_CALLBACK_URL", "http://api-green:3000/internal/external-executions/events")
    monkeypatch.setenv("SSPANEL_CALLBACK_SECRET", "callback-secret")
    store = sspanel.SspanelExecutorStore(str(tmp_path / "outbox.sqlite3"))
    try:
        request = sspanel.JobStartRequest(
            idempotencyKey="outbox:job:1",
            orderId=1,
            platform="instagram",
            actionType="instagram.account.health",
            quantity=1,
            accountSelector={"mode": "system"},
            payload={},
        )
        job = store.create_job(request)
        rows = store.outbox.all()
        assert len(rows) == 1
        assert rows[0]["payload"]["event"]["eventType"] == "job.accepted"

        claimed = store.claim_outbox("worker-1")
        assert claimed is not None
        assert claimed["attempts"] == 1
        assert claimed["status"] == "delivering"
        store.mark_outbox_delivered(claimed["outboxId"])
        assert store.outbox.get(sspanel.Query().outboxId == claimed["outboxId"])["status"] == "delivered"
        assert store.find_job(job["jobId"])["eventSequence"] == 1
    finally:
        store.close()


@pytest.mark.asyncio
async def test_executor_outbox_delivery_retries_without_logging_callback_secret(tmp_path, monkeypatch):
    monkeypatch.setenv("SSPANEL_CALLBACK_URL", "http://api-green:3000/internal/external-executions/events")
    monkeypatch.setenv("SSPANEL_CALLBACK_SECRET", "callback-secret")
    store = sspanel.SspanelExecutorStore(str(tmp_path / "outbox-delivery.sqlite3"))
    calls = []

    class Response:
        status_code = 202

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        return Response()

    monkeypatch.setattr(sspanel.requests, "post", fake_post)
    try:
        store.create_job(sspanel.JobStartRequest(
            idempotencyKey="outbox:delivery:1",
            orderId=1,
            platform="instagram",
            actionType="instagram.account.health",
            quantity=1,
            accountSelector={"mode": "system"},
            payload={},
        ))
        worker = sspanel.ExecutorWorker(store, worker_id="worker-1")
        assert await worker.deliver_outbox_once() is True
        delivered = store.outbox.all()[0]
        assert delivered["status"] == "delivered"
        assert calls[0][0] == "http://api-green:3000/internal/external-executions/events"
        assert calls[0][1]["headers"]["X-External-Executor-Callback-Secret"] == "callback-secret"
    finally:
        store.close()


@pytest.mark.asyncio
async def test_sspanel_job_status_is_toolkit_compatible():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await _post_job(client)
        job_id = created.json()["jobId"]
        status = await _wait_for_terminal(client, job_id)

    assert status.status_code == 200
    assert created.json()["status"] == "queued"
    assert status.json()["jobId"] == job_id
    assert status.json()["status"] == "failed"
    assert status.json()["errorCode"] == "executor_no_terminal_transition"
    assert status.json()["completedCount"] == 0
    assert status.json()["totalCount"] == 1
    assert status.json()["result"] == {}
    assert status.json()["accountHealth"] == {}


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
        status = await _wait_for_terminal(client, job_id)

    assert created.status_code == 202
    assert created.json()["status"] == "queued"
    assert status.json()["status"] == "completed"
    assert status.json()["completedCount"] == 1
    assert status.json()["totalCount"] == 1
    assert status.json()["result"] == {"ok": True}
    assert status.json()["accountHealth"] == {
        "executorAccountId": account_id,
        "status": "healthy",
        "username": "ig_user",
    }
    events = status.json()["events"]
    assert [event["eventType"] for event in events] == [
        "job.accepted",
        "job.started",
        "job.completed",
        "action.completed",
    ]
    assert [event["sequence"] for event in events] == [1, 2, 3, 4]
    assert status.json()["eventSequence"] == 4
    assert fake_client.calls == ["account_info"]
    assert factory_accounts[0]["executorAccountId"] == account_id
    assert factory_accounts[0]["sessionid"] == "raw-session-secret"


@pytest.mark.asyncio
async def test_sspanel_policy_envelope_pauses_before_executor_action_when_limit_is_reached(monkeypatch):
    fake_client = FakeHealthClient()
    monkeypatch.setattr(sspanel, "create_instagram_health_client", lambda _account: fake_client, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        imported = await _import_session(client)
        account_id = imported.json()["executorAccountId"]
        created = await _post_job(
            client,
            idempotency_key="instagram:policy:blocked:1",
            account_selector={"mode": "specific", "accountIds": [account_id]},
            policy_envelope={
                "policyVersion": "instagram-safe-v1",
                "riskProfile": "safe",
                "actionLimits": {"instagram.account.health": {"hourly": 0}},
            },
        )
        status = await _wait_for_terminal_or_paused(client, created.json()["jobId"])

    assert created.json()["status"] == "queued"
    assert status.json()["status"] == "paused"
    assert status.json()["errorCode"] == "POLICY_LIMIT_REACHED"
    assert status.json()["events"][-1]["eventType"] == "workflow.paused"
    assert fake_client.calls == []


@pytest.mark.asyncio
async def test_sspanel_warmup_runs_checkpointed_action_mix_and_reports_next_run(monkeypatch):
    fake_client = FakeHealthClient()
    monkeypatch.setenv("SSPANEL_EXECUTOR_WARMUP_PACING_SECONDS", "0")
    monkeypatch.setattr(sspanel, "create_instagram_health_client", lambda _account: fake_client, raising=False)
    monkeypatch.setattr(sspanel, "create_instagram_profile_client", lambda _account: fake_client, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        imported = await _import_session(client)
        account_id = imported.json()["executorAccountId"]
        created = await _post_job(
            client,
            idempotency_key="instagram:warmup:1",
            action_type="instagram.warmup",
            quantity=2,
            account_selector={"mode": "specific", "accountIds": [account_id]},
            payload={
                "actionMix": ["instagram.account.health", "instagram.profile.get"],
                "targetIds": ["target_user"],
            },
        )
        status = await _wait_for_terminal(client, created.json()["jobId"])

    assert created.json()["status"] == "queued"
    assert status.json()["status"] == "completed"
    assert status.json()["completedCount"] == 2
    assert status.json()["result"]["phase"] == "warmup"
    assert status.json()["result"]["cursor"] == 2
    assert fake_client.calls == ["account_info", ("user_info_by_username", "target_user")]


@pytest.mark.asyncio
async def test_sspanel_profile_get_completes_for_imported_specific_account(monkeypatch):
    fake_client = FakeHealthClient()
    factory_accounts = []

    def fake_factory(account):
        factory_accounts.append(account)
        return fake_client

    monkeypatch.setattr(sspanel, "create_instagram_profile_client", fake_factory, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        imported = await _import_session(client)
        account_id = imported.json()["executorAccountId"]
        created = await _post_job(
            client,
            idempotency_key="instagram:profile:1",
            action_type="instagram.profile.get",
            account_selector={"mode": "specific", "accountIds": [account_id]},
            payload={"username": "@target_user"},
        )
        job_id = created.json()["jobId"]
        status = await _wait_for_terminal(client, job_id)

    assert created.status_code == 202
    assert created.json()["status"] == "queued"
    assert status.json()["status"] == "completed"
    assert status.json()["result"] == {
        "profile": {"pk": 456, "username": "target_user", "full_name": "Profile User"},
        "executorAccountId": account_id,
    }
    assert fake_client.calls == [("user_info_by_username", "target_user")]
    assert factory_accounts[0]["sessionid"] == "raw-session-secret"


@pytest.mark.asyncio
async def test_sspanel_comments_list_returns_normalized_page(monkeypatch):
    fake_client = FakeHealthClient()
    monkeypatch.setattr(sspanel, "create_instagram_comments_client", lambda _account: fake_client, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        imported = await _import_session(client)
        account_id = imported.json()["executorAccountId"]
        created = await _post_job(
            client,
            idempotency_key="instagram:comments:list:1",
            action_type="instagram.comments.list",
            account_selector={"mode": "specific", "accountIds": [account_id]},
            payload={"mediaId": "media-1", "amount": 10},
        )
        status = await _wait_for_terminal(client, created.json()["jobId"])

    assert created.json()["status"] == "queued"
    assert status.json()["result"] == {
        "mediaId": "media-1",
        "comments": [{"pk": 11, "text": "Hello"}],
        "executorAccountId": account_id,
        "nextCursor": "next-page",
    }
    assert fake_client.calls == [("media_comments_chunk", "media-1", 10, None)]


@pytest.mark.asyncio
async def test_sspanel_comments_reply_uses_idempotent_write_job_and_releases_lease(monkeypatch):
    fake_client = FakeHealthClient()
    monkeypatch.setattr(sspanel, "create_instagram_comments_client", lambda _account: fake_client, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        imported = await _import_session(client)
        account_id = imported.json()["executorAccountId"]
        payload = {"mediaId": "media-1", "text": "Thanks", "commentId": "11"}
        first = await _post_job(
            client,
            idempotency_key="instagram:comments:reply:1",
            action_type="instagram.comments.reply",
            account_selector={"mode": "specific", "accountIds": [account_id]},
            payload=payload,
        )
        second = await _post_job(
            client,
            idempotency_key="instagram:comments:reply:1",
            action_type="instagram.comments.reply",
            account_selector={"mode": "specific", "accountIds": [account_id]},
            payload=payload,
        )

    assert first.json()["status"] == "queued"
    assert second.json()["jobId"] == first.json()["jobId"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        completed = await _wait_for_terminal(client, first.json()["jobId"])
    assert completed.json()["status"] == "completed"
    assert fake_client.calls == [("media_comment", "media-1", "Thanks", 11)]


@pytest.mark.asyncio
async def test_sspanel_smart_comments_workflow_waits_for_core_content_then_publishes(monkeypatch):
    fake_client = FakeHealthClient()
    monkeypatch.setattr(sspanel, "create_instagram_comments_client", lambda _account: fake_client, raising=False)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        imported = await _import_session(client)
        account_id = imported.json()["executorAccountId"]
        created = await _post_job(
            client,
            idempotency_key="instagram:smart-comments:1",
            action_type="instagram.comments.smart_reply",
            account_selector={"mode": "specific", "accountIds": [account_id]},
            payload={"mediaIds": ["media-1"], "scenarioRef": "funnel:general"},
        )
        job_id = created.json()["jobId"]
        awaiting = await _wait_for_terminal_or_paused(client, job_id)
        input_body = {
            "inputIdempotencyKey": "instagram:smart-comments:1:content:v1",
            "decisions": [{
                "candidateId": "media-1:11",
                "text": "Thanks for sharing",
                "decision": "publish",
            }],
        }
        published = await client.post(
            f"/module/v1/jobs/{job_id}/input",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
            json=input_body,
        )
        accepted = published
        published = await _wait_for_terminal(client, job_id)
        repeated = await client.post(
            f"/module/v1/jobs/{job_id}/input",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
            json=input_body,
        )

    assert created.json()["status"] == "queued"
    assert awaiting.json()["result"]["phase"] == "awaiting_content"
    assert awaiting.json()["result"]["candidates"][0]["candidateId"] == "media-1:11"
    assert accepted.json()["status"] == "queued"
    assert published.json()["status"] == "completed"
    assert published.json()["completedCount"] == 1
    assert repeated.json()["status"] == "completed"
    action_events = [event for event in published.json()["events"] if event["eventType"] == "action.completed"]
    assert action_events[-1]["metadata"]["publishedCandidateIds"] == ["media-1:11"]
    assert fake_client.calls == [
        ("media_comments_chunk", "media-1", 1, None),
        ("media_comment", "media-1", "Thanks for sharing", 11),
    ]


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
        status = await _wait_for_terminal(client, job_id)

    body = status.json()
    assert created.status_code == 202
    assert created.json()["status"] == "queued"
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
async def test_module_manifest_advertises_only_implemented_capabilities():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        unauthorized = await client.get("/module/v1/manifest")
        response = await client.get(
            "/module/v1/manifest",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
        )

    assert unauthorized.status_code == 401
    assert response.status_code == 200
    body = response.json()
    input_schemas = body.pop("inputSchemas")
    assert body == {
        "moduleId": "instagram-executor",
        "platform": "instagram",
        "contractVersions": ["1.0", "1.1"],
        "features": [
            "event-sequence.v1",
            "managed-workflows.v1",
            "pause-resume.v1",
            "provider-retry-confirmation.v1",
            "workflow-input.v1",
        ],
        "capabilities": [
            "instagram.account.health",
            "instagram.comments.delete",
            "instagram.comments.list",
            "instagram.comments.pin",
            "instagram.comments.reply",
            "instagram.comments.smart_reply",
            "instagram.dm.inbox",
            "instagram.dm.reply",
            "instagram.dm.send",
            "instagram.insights.basic",
            "instagram.media.upload.photo",
            "instagram.media.upload.reel",
            "instagram.media.upload.video",
            "instagram.profile.get",
            "instagram.story.upload",
            "instagram.warmup",
        ],
        "workflowTypes": ["instagram.comments.smart_reply", "instagram.warmup"],
        "supportsPolling": True,
        "supportsCallbacks": False,
    }
    assert input_schemas["instagram.comments.reply"]["properties"]["mediaId"] == {
        "type": "string",
        "title": "Media ID",
    }
    assert input_schemas["instagram.story.upload"]["properties"]["mediaType"]["enum"] == ["photo", "video"]


@pytest.mark.asyncio
async def test_executor_auth_reads_api_key_from_mounted_secret_file(tmp_path, monkeypatch):
    key_file = tmp_path / "executor-api-key"
    key_file.write_text("executor-secret")
    monkeypatch.delenv("SSPANEL_EXECUTOR_API_KEY", raising=False)
    monkeypatch.setenv("SSPANEL_EXECUTOR_API_KEY_FILE", str(key_file))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(
            "/module/v1/manifest",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
        )

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_manifest_advertises_callbacks_only_when_url_and_secret_are_configured(monkeypatch):
    monkeypatch.setenv("SSPANEL_CALLBACK_SECRET", "callback-secret")
    monkeypatch.delenv("SSPANEL_CALLBACK_URL", raising=False)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        without_url = await client.get(
            "/module/v1/manifest",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
        )
    assert without_url.json()["supportsCallbacks"] is False

    monkeypatch.setenv("SSPANEL_CALLBACK_URL", "http://api-green:3000/internal/external-executions/events")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        with_url = await client.get(
            "/module/v1/manifest",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
        )
    assert with_url.json()["supportsCallbacks"] is True


@pytest.mark.asyncio
async def test_module_api_rejects_invalid_payload_for_advertised_capability():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await _post_job(
            client,
            action_type="instagram.media.upload.photo",
            payload={},
            account_selector={"mode": "system"},
        )

    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action_type", "payload"),
    [
        ("instagram.account.health", {"unexpected": True}),
        ("instagram.profile.get", {"username": "one", "userId": "2"}),
        ("instagram.profile.get", {"username": " "}),
        ("instagram.comments.list", {}),
        ("instagram.comments.list", {"mediaId": " "}),
        ("instagram.comments.reply", {"mediaId": "media-1", "text": "reply", "commentId": "not-numeric"}),
        ("instagram.comments.reply", {"mediaId": "media-1", "text": " "}),
        ("instagram.comments.smart_reply", {"mediaIds": []}),
        ("instagram.warmup", {"actionMix": ["instagram.not-a-warmup-action"]}),
        ("instagram.warmup", {"actionMix": ["instagram.comments.list"]}),
    ],
)
async def test_module_api_rejects_invalid_typed_payloads(action_type, payload):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await _post_job(
            client,
            idempotency_key=f"conformance:invalid:{action_type}:v1",
            action_type=action_type,
            payload=payload,
        )

    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "media_url",
    [
        "http://127.0.0.1/internal.jpg",
        "http://localhost/internal.jpg",
        "https://user:pass@cdn.example.test/private.jpg",
    ],
)
async def test_media_capability_rejects_private_or_credentialed_urls(media_url):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await _post_job(
            client,
            action_type="instagram.media.upload.photo",
            payload={"mediaUrl": media_url},
        )

    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action_type", "payload", "expected_call", "result_key"),
    [
        (
            "instagram.media.upload.photo",
            {"mediaUrl": "https://cdn.example.test/photo.jpg", "caption": "photo"},
            "photo_upload",
            "media",
        ),
        (
            "instagram.media.upload.video",
            {"mediaUrl": "https://cdn.example.test/video.mp4", "caption": "video"},
            "video_upload",
            "media",
        ),
        (
            "instagram.media.upload.reel",
            {"mediaUrl": "https://cdn.example.test/reel.mp4", "caption": "reel"},
            "clip_upload",
            "media",
        ),
        (
            "instagram.story.upload",
            {"mediaUrl": "https://cdn.example.test/story.jpg", "mediaType": "photo", "caption": "story"},
            "photo_upload_to_story",
            "story",
        ),
    ],
)
async def test_media_capabilities_execute_with_private_temp_download(
    monkeypatch,
    tmp_path,
    action_type,
    payload,
    expected_call,
    result_key,
):
    client_stub = FakeCapabilityClient()
    monkeypatch.setattr(sspanel, "create_instagram_media_client", lambda _account: client_stub)

    def fake_download(_url, suffix):
        path = tmp_path / f"input{suffix}"
        path.write_bytes(b"test-media")
        return path

    monkeypatch.setattr(sspanel, "_download_media_to_temp", fake_download)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        _, status = await _start_specific_job(
            client,
            action_type,
            payload,
            idempotency_key=f"conformance:{action_type}:v1",
        )

    assert status.json()["status"] == "completed"
    assert status.json()["completedCount"] == 1
    assert result_key in status.json()["result"]
    assert client_stub.calls[0][0] == expected_call


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action_type", "payload", "expected_call", "completed_count"),
    [
        (
            "instagram.comments.delete",
            {"mediaId": "media-1", "commentIds": ["11", "12"]},
            "comment_bulk_delete",
            2,
        ),
        (
            "instagram.comments.pin",
            {"mediaId": "media-1", "commentIds": ["11"]},
            "comment_pin",
            1,
        ),
    ],
)
async def test_comment_moderation_capabilities_execute(
    monkeypatch,
    action_type,
    payload,
    expected_call,
    completed_count,
):
    client_stub = FakeCapabilityClient()
    monkeypatch.setattr(sspanel, "create_instagram_comments_client", lambda _account: client_stub)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        _, status = await _start_specific_job(
            client,
            action_type,
            payload,
            idempotency_key=f"conformance:{action_type}:v1",
        )

    assert status.json()["status"] == "completed"
    assert status.json()["completedCount"] == completed_count
    assert client_stub.calls[0][0] == expected_call


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action_type", "payload", "expected_call"),
    [
        ("instagram.dm.inbox", {"amount": 5, "selectedFilter": "unread"}, "direct_threads"),
        ("instagram.dm.send", {"text": "hello", "userIds": ["201"]}, "direct_send"),
        ("instagram.dm.reply", {"threadId": "301", "text": "reply"}, "direct_answer"),
    ],
)
async def test_dm_capabilities_execute(monkeypatch, action_type, payload, expected_call):
    client_stub = FakeCapabilityClient()
    monkeypatch.setattr(sspanel, "create_instagram_direct_client", lambda _account: client_stub)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        _, status = await _start_specific_job(
            client,
            action_type,
            payload,
            idempotency_key=f"conformance:{action_type}:v1",
        )

    assert status.json()["status"] == "completed"
    assert status.json()["result"]["executorAccountId"].startswith("ig_acc_")
    assert client_stub.calls[0][0] == expected_call


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expected_call", "scope"),
    [
        ({}, "insights_account", "account"),
        ({"mediaId": "media-1"}, "insights_media", "media"),
    ],
)
async def test_basic_insights_capability_execute(monkeypatch, payload, expected_call, scope):
    client_stub = FakeCapabilityClient()
    monkeypatch.setattr(sspanel, "create_instagram_insights_client", lambda _account: client_stub)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        _, status = await _start_specific_job(
            client,
            "instagram.insights.basic",
            payload,
            idempotency_key=f"conformance:instagram.insights.basic:{scope}:v1",
        )

    assert status.json()["status"] == "completed"
    assert status.json()["result"]["scope"] == scope
    assert client_stub.calls[0][0] == expected_call


@pytest.mark.asyncio
async def test_legacy_sspanel_routes_remain_available():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/sspanel/jobs",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
            json={
                "idempotencyKey": "instagram:legacy:1",
                "orderId": 1,
                "platform": "instagram",
                "actionType": "instagram.account.health",
                "quantity": 1,
                "accountSelector": {"mode": "system"},
                "payload": {},
            },
        )

    assert response.status_code == 202


@pytest.mark.asyncio
async def test_module_health_is_public():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/module/v1/health")

    assert response.status_code == 200
    assert response.json() == {
        "moduleId": "instagram-executor",
        "status": "ok",
    }


@pytest.mark.asyncio
async def test_sspanel_account_import_does_not_log_sensitive_values(caplog):
    caplog.set_level(logging.INFO)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/module/v1/accounts/import-session",
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


@pytest.mark.asyncio
async def test_sspanel_account_delete_route_is_idempotent_for_missing_remote_account():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        imported = await client.post(
            "/module/v1/accounts/import-session",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
            json={"username": "delete-route-user", "sessionid": "delete-route-session"},
        )
        account_id = imported.json()["executorAccountId"]
        deleted = await client.delete(
            f"/module/v1/accounts/{account_id}",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
        )
        repeated = await client.delete(
            f"/module/v1/accounts/{account_id}",
            headers={"X-SSPanel-Executor-Key": "executor-secret"},
        )

    assert imported.status_code == 200
    assert deleted.status_code == 200
    assert deleted.json() == {"executorAccountId": account_id, "deleted": True}
    assert repeated.status_code == 404


def test_executor_account_delete_removes_encrypted_account_and_usage(tmp_path):
    store = sspanel.SspanelExecutorStore(str(tmp_path / "account-delete.sqlite3"))
    try:
        account = store.import_account(sspanel.ImportSessionRequest(
            username="delete_user",
            sessionid="raw-session-secret",
            proxy="http://proxy-user:proxy-pass@example.test:8000",
        ))
        store.usage.insert({
            "usageKey": "ig_acc_usage:daily",
            "accountId": account["executorAccountId"],
            "used": 3,
        })

        result = store.delete_account(account["executorAccountId"])

        assert result == {"executorAccountId": account["executorAccountId"], "deleted": True}
        assert store.get_account(account["executorAccountId"]) is None
        assert store.usage.all() == []
    finally:
        store.close()


def test_executor_account_delete_blocks_active_assigned_jobs(tmp_path):
    store = sspanel.SspanelExecutorStore(str(tmp_path / "account-delete-active.sqlite3"))
    try:
        account_id = store.import_account(sspanel.ImportSessionRequest(username="active_user"))["executorAccountId"]
        request = sspanel.JobStartRequest(
            idempotencyKey="instagram:account-delete:active",
            orderId=1,
            platform="instagram",
            actionType="instagram.comments.reply",
            quantity=1,
            accountSelector={"mode": "specific", "accountIds": [account_id]},
            payload={"mediaId": "media-1", "text": "reply"},
        )
        store.create_job(request)

        with pytest.raises(HTTPException) as error:
            store.delete_account(account_id)
        assert error.value.status_code == 409
        assert store.get_account(account_id) is not None
    finally:
        store.close()


def test_executor_account_write_lease_blocks_until_terminal_job(tmp_path):
    store = sspanel.SspanelExecutorStore(str(tmp_path / "leases.json"))
    try:
        account = store.import_account(sspanel.ImportSessionRequest(username="lease_user"))
        first = sspanel.JobStartRequest(
            idempotencyKey="instagram:write:1",
            orderId=1,
            platform="instagram",
            actionType="instagram.media.upload.photo",
            quantity=1,
            accountSelector={"mode": "specific", "accountIds": [account["executorAccountId"]]},
            payload={"mediaUrl": "https://cdn.example.test/photo.jpg"},
        )
        first_job = store.create_job(first)

        second = first.model_copy(update={"idempotencyKey": "instagram:write:2", "orderId": 2})
        with pytest.raises(HTTPException) as error:
            store.create_job(second)
        assert error.value.status_code == 409

        store.update_job(first_job["jobId"], {"status": "completed"})
        third = first.model_copy(update={"idempotencyKey": "instagram:write:3", "orderId": 3})
        released_job = store.create_job(third)
        assert released_job["assignedAccountIds"] == [account["executorAccountId"]]
    finally:
        store.close()


def test_executor_operator_pause_and_resume_are_idempotent(tmp_path):
    store = sspanel.SspanelExecutorStore(str(tmp_path / "operator-pause.json"))
    try:
        request = sspanel.JobStartRequest(
            idempotencyKey="instagram:operator-pause:1",
            orderId=1,
            platform="instagram",
            actionType="instagram.account.health",
            quantity=1,
            accountSelector={"mode": "system"},
            payload={},
        )
        created = store.create_job(request)

        paused = store.pause_job(created["jobId"])
        repeated_pause = store.pause_job(created["jobId"])
        assert paused["status"] == "paused"
        assert paused["pauseReason"] == "operator"
        assert paused["nextRunAt"] is None
        assert repeated_pause["jobId"] == paused["jobId"]
        assert repeated_pause["status"] == "paused"

        resumed = store.resume_job(created["jobId"])
        repeated_resume = store.resume_job(created["jobId"])
        assert resumed["status"] == "queued"
        assert resumed["pauseReason"] is None
        assert resumed["nextRunAt"]
        assert repeated_resume["jobId"] == resumed["jobId"]
        assert repeated_resume["status"] == "queued"
    finally:
        store.close()


def test_executor_worker_claim_and_restart_recovery_are_persisted(tmp_path):
    path = tmp_path / "worker-recovery.json"
    request = sspanel.JobStartRequest(
        idempotencyKey="instagram:worker:recovery:1",
        orderId=1,
        platform="instagram",
        actionType="instagram.account.health",
        quantity=1,
        accountSelector={"mode": "system"},
        payload={},
    )

    first_store = sspanel.SspanelExecutorStore(str(path))
    created = first_store.create_job(request)
    claimed = first_store.claim_next_job("worker-a")
    assert created["status"] == "queued"
    assert claimed["status"] == "running"
    assert claimed["attempts"] == 1
    first_store.close()

    recovered_store = sspanel.SspanelExecutorStore(str(path))
    try:
        assert recovered_store.recover_jobs("worker-b") == 1
        recovered = recovered_store.find_job(created["jobId"])
        assert recovered["status"] == "queued"
        assert recovered["workerId"] is None
        reclaimed = recovered_store.claim_next_job("worker-b")
        assert reclaimed["status"] == "running"
        assert reclaimed["attempts"] == 2
    finally:
        recovered_store.close()


def test_executor_does_not_auto_retry_an_uncertain_provider_write(tmp_path):
    path = tmp_path / "uncertain-write.json"
    first_store = sspanel.SspanelExecutorStore(str(path))
    try:
        account_id = first_store.import_account(sspanel.ImportSessionRequest(username="uncertain_user"))["executorAccountId"]
        request = sspanel.JobStartRequest(
            idempotencyKey="instagram:worker:uncertain-write:1",
            orderId=1,
            platform="instagram",
            actionType="instagram.comments.reply",
            quantity=1,
            accountSelector={"mode": "specific", "accountIds": [account_id]},
            payload={"mediaId": "media-1", "text": "reply"},
        )
        created = first_store.create_job(request)
        claimed = first_store.claim_next_job("worker-a")
        assert claimed["status"] == "running"
        first_store.update_job(created["jobId"], {"providerCallStartedAt": sspanel._utc_now()})
    finally:
        first_store.close()

    recovered_store = sspanel.SspanelExecutorStore(str(path))
    try:
        assert recovered_store.recover_jobs("worker-b") == 1
        recovered = recovered_store.find_job(created["jobId"])
        assert recovered["status"] == "paused"
        assert recovered["pauseReason"] == "write_reconciliation_required"
        assert recovered["errorCode"] == "WRITE_RECONCILIATION_REQUIRED"
        with pytest.raises(HTTPException) as error:
            recovered_store.resume_job(created["jobId"])
        assert error.value.status_code == 409

        resumed = recovered_store.resume_job(created["jobId"], confirm_provider_retry=True)
        assert resumed["status"] == "queued"
        assert resumed["pauseReason"] is None
    finally:
        recovered_store.close()


def test_executor_worker_does_not_claim_cancelled_jobs(tmp_path):
    store = sspanel.SspanelExecutorStore(str(tmp_path / "cancelled.json"))
    try:
        request = sspanel.JobStartRequest(
            idempotencyKey="instagram:worker:cancelled:1",
            orderId=1,
            platform="instagram",
            actionType="instagram.account.health",
            quantity=1,
            accountSelector={"mode": "system"},
            payload={},
        )
        created = store.create_job(request)
        assert store.cancel_job(created["jobId"])["status"] == "cancelled"
        assert store.claim_next_job("worker-a") is None
    finally:
        store.close()
