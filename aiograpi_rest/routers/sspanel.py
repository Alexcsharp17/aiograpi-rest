import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional

from aiograpi import Client
from fastapi import APIRouter, Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field
from tinydb import Query, TinyDB

logger = logging.getLogger("aiograpi_rest.sspanel")

router = APIRouter(
    prefix="/sspanel",
    tags=["SS-panel"],
    responses={404: {"description": "Not found"}},
)

Platform = Literal["instagram"]
JobStatus = Literal[
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
]
ActionType = Literal[
    "instagram.account.health",
    "instagram.profile.get",
    "instagram.media.upload.photo",
    "instagram.media.upload.video",
    "instagram.media.upload.reel",
    "instagram.story.upload",
    "instagram.comments.list",
    "instagram.comments.reply",
    "instagram.comments.delete",
    "instagram.comments.pin",
    "instagram.dm.inbox",
    "instagram.dm.send",
    "instagram.dm.reply",
    "instagram.insights.basic",
]

WRITE_ACTIONS = {
    "instagram.media.upload.photo",
    "instagram.media.upload.video",
    "instagram.media.upload.reel",
    "instagram.story.upload",
    "instagram.comments.reply",
    "instagram.comments.delete",
    "instagram.comments.pin",
    "instagram.dm.send",
    "instagram.dm.reply",
}
ACTIVE_WRITE_STATUSES = {"queued", "running", "paused", "rate_limited"}
SENSITIVE_KEY_PARTS = ("password", "session", "cookie", "proxy", "token", "secret")

_store: Optional["SspanelExecutorStore"] = None
sspanel_executor_key_header = APIKeyHeader(
    name="X-SSPanel-Executor-Key",
    scheme_name="SspanelExecutorKey",
    description="SS-panel executor API key for /sspanel/* facade routes.",
    auto_error=False,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _store_path() -> str:
    explicit = os.getenv("SSPANEL_EXECUTOR_DB_PATH")
    if explicit:
        return explicit
    aiograpi_db = os.getenv("AIOGRAPI_REST_DB_PATH")
    if aiograpi_db:
        return str(Path(aiograpi_db).with_name("sspanel-executor.json"))
    return "/data/sspanel-executor.json"


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, child in value.items():
            if any(part in key.lower() for part in SENSITIVE_KEY_PARTS):
                redacted[key] = "[REDACTED]"
            else:
                redacted[key] = _redact(child)
        return redacted
    if isinstance(value, list):
        return [_redact(child) for child in value]
    return value


def _stable_id(prefix: str, value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _jsonable(child) for key, child in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(child) for child in value]
    if hasattr(value, "model_dump"):
        return _jsonable(value.model_dump())
    if hasattr(value, "dict"):
        return _jsonable(value.dict())
    return str(value)


def _exception_status(error: Exception) -> JobStatus:
    message = str(error).lower()
    name = error.__class__.__name__.lower()
    combined = f"{name} {message}"
    if "challenge" in combined or "checkpoint" in combined:
        return "challenge_required"
    if "login_required" in combined or "login required" in combined or "session" in combined:
        return "session_expired"
    if "banned" in combined or "locked" in combined or "suspended" in combined:
        return "banned_or_locked"
    if "rate" in combined or "please wait" in combined or "feedback_required" in combined:
        return "rate_limited"
    return "failed"


def create_instagram_health_client(account: dict[str, Any]) -> Client:
    client = Client()
    settings = account.get("settings")
    if settings:
        client.set_settings(settings)
    proxy = account.get("proxy")
    if proxy:
        client.set_proxy(proxy)
    return client


class AccountSelector(BaseModel):
    mode: Literal["system", "specific"]
    accountIds: Optional[list[str]] = None
    constraints: Optional[dict[str, Any]] = None


class JobStartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotencyKey: str = Field(..., min_length=1)
    orderId: int
    platform: Platform
    actionType: ActionType
    quantity: int = Field(..., ge=1)
    accountSelector: AccountSelector
    payload: dict[str, Any] = Field(default_factory=dict)
    callbackUrl: Optional[str] = None


class JobStartResponse(BaseModel):
    jobId: str
    status: JobStatus
    acceptedAt: str


class JobProgressResponse(BaseModel):
    jobId: str
    status: JobStatus
    completedCount: int
    totalCount: int
    errorCode: Optional[str] = None
    errorMessage: Optional[str] = None
    result: dict[str, Any] = Field(default_factory=dict)
    accountHealth: dict[str, Any] = Field(default_factory=dict)


class ImportSessionRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    username: Optional[str] = None
    sessionid: Optional[str] = None
    settings: Optional[dict[str, Any]] = None
    proxy: Optional[str] = None


class ImportSessionResponse(BaseModel):
    executorAccountId: str
    status: str


class SspanelExecutorStore:
    def __init__(self, path: str):
        db_path = Path(path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = TinyDB(str(db_path))
        self.jobs = self.db.table("jobs")
        self.accounts = self.db.table("accounts")

    def close(self) -> None:
        self.db.close()

    def find_job_by_idempotency(self, idempotency_key: str) -> Optional[dict[str, Any]]:
        rows = self.jobs.search(Query().idempotencyKey == idempotency_key)
        return rows[0] if rows else None

    def find_job(self, job_id: str) -> Optional[dict[str, Any]]:
        rows = self.jobs.search(Query().jobId == job_id)
        return rows[0] if rows else None

    def create_job(self, request: JobStartRequest) -> dict[str, Any]:
        existing = self.find_job_by_idempotency(request.idempotencyKey)
        if existing:
            return existing
        self._assert_write_capacity(request)
        accepted_at = _utc_now()
        job = {
            "jobId": _stable_id("ig_job", request.idempotencyKey),
            "idempotencyKey": request.idempotencyKey,
            "orderId": request.orderId,
            "platform": request.platform,
            "actionType": request.actionType,
            "quantity": request.quantity,
            "accountSelector": request.accountSelector.model_dump(exclude_none=True),
            "payload": request.payload,
            "callbackUrl": request.callbackUrl,
            "status": "queued",
            "completedCount": 0,
            "totalCount": request.quantity,
            "result": {},
            "accountHealth": {},
            "acceptedAt": accepted_at,
            "updatedAt": accepted_at,
        }
        self.jobs.insert(job)
        return job

    def update_job(self, job_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        job = self.find_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="SS-panel executor job not found")
        updated = {**job, **updates, "updatedAt": _utc_now()}
        self.jobs.update(updated, Query().jobId == job_id)
        return updated

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        job = self.find_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="SS-panel executor job not found")
        cancelled = {**job, "status": "cancelled", "updatedAt": _utc_now()}
        self.jobs.update(cancelled, Query().jobId == job_id)
        return cancelled

    def import_account(self, request: ImportSessionRequest) -> dict[str, Any]:
        identity = request.username or request.sessionid or json.dumps(request.settings or {}, sort_keys=True)
        if not identity:
            raise HTTPException(status_code=422, detail="username, sessionid, or settings is required")
        account_id = _stable_id("ig_acc", identity)
        row = {
            "executorAccountId": account_id,
            "username": request.username,
            "sessionid": request.sessionid,
            "settings": request.settings,
            "proxy": request.proxy,
            "status": "imported",
            "updatedAt": _utc_now(),
        }
        self.accounts.upsert(row, Query().executorAccountId == account_id)
        logger.info("Imported SS-panel Instagram account: %s", _redact(row))
        return row

    def get_account(self, account_id: str) -> Optional[dict[str, Any]]:
        rows = self.accounts.search(Query().executorAccountId == account_id)
        return rows[0] if rows else None

    def resolve_specific_account(self, request: JobStartRequest) -> Optional[dict[str, Any]]:
        if request.accountSelector.mode != "specific":
            return None
        account_id = (request.accountSelector.accountIds or [None])[0]
        if not account_id:
            raise HTTPException(status_code=422, detail="specific accountSelector requires accountIds")
        account = self.get_account(account_id)
        if not account:
            raise HTTPException(status_code=404, detail="SS-panel executor account not found")
        return account

    def _assert_write_capacity(self, request: JobStartRequest) -> None:
        if request.actionType not in WRITE_ACTIONS:
            return
        if request.accountSelector.mode != "specific":
            return
        account_ids = set(request.accountSelector.accountIds or [])
        if not account_ids:
            return
        for row in self.jobs.all():
            if row.get("actionType") not in WRITE_ACTIONS:
                continue
            if row.get("status") not in ACTIVE_WRITE_STATUSES:
                continue
            row_selector = row.get("accountSelector") or {}
            row_account_ids = set(row_selector.get("accountIds") or [])
            if account_ids & row_account_ids:
                raise HTTPException(status_code=409, detail="Instagram account already has an active write job")


def get_store() -> SspanelExecutorStore:
    global _store
    if _store is None:
        _store = SspanelExecutorStore(_store_path())
    return _store


def reset_store_for_tests() -> None:
    global _store
    if _store is not None:
        _store.close()
        _store = None


def require_sspanel_api_key(x_sspanel_executor_key: Optional[str] = Security(sspanel_executor_key_header)) -> None:
    expected = os.getenv("SSPANEL_EXECUTOR_API_KEY", "")
    if not expected or x_sspanel_executor_key != expected:
        raise HTTPException(status_code=401, detail="Invalid SS-panel executor API key")


async def execute_account_health_job(
    job: dict[str, Any],
    request: JobStartRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    if request.actionType != "instagram.account.health":
        return job
    account = store.resolve_specific_account(request)
    if not account:
        return job

    try:
        client = create_instagram_health_client(account)
        if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
            await client.login_by_sessionid(account["sessionid"])
        raw_info = await client.account_info()
        info = _redact(_jsonable(raw_info))
        username = info.get("username") if isinstance(info, dict) else account.get("username")
        return store.update_job(
            job["jobId"],
            {
                "status": "completed",
                "completedCount": 1,
                "totalCount": job.get("totalCount", request.quantity),
                "result": {"ok": True},
                "accountHealth": {
                    "executorAccountId": account["executorAccountId"],
                    "status": "healthy",
                    **({"username": username} if username else {}),
                },
            },
        )
    except Exception as error:
        external_status = _exception_status(error)
        return store.update_job(
            job["jobId"],
            {
                "status": external_status,
                "completedCount": 0,
                "totalCount": job.get("totalCount", request.quantity),
                "errorCode": external_status,
                "errorMessage": str(error),
                "result": {},
                "accountHealth": {
                    "executorAccountId": account["executorAccountId"],
                    "status": external_status,
                },
            },
        )


@router.post(
    "/jobs",
    response_model=JobStartResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_sspanel_api_key)],
)
async def start_job(request: JobStartRequest, store: SspanelExecutorStore = Depends(get_store)) -> JobStartResponse:
    job = store.create_job(request)
    job = await execute_account_health_job(job, request, store)
    logger.info(
        "Accepted SS-panel Instagram job: %s",
        _redact({"jobId": job["jobId"], "orderId": job["orderId"], "actionType": job["actionType"]}),
    )
    return JobStartResponse(jobId=job["jobId"], status=job["status"], acceptedAt=job["acceptedAt"])


@router.get(
    "/jobs/{job_id}",
    response_model=JobProgressResponse,
    response_model_exclude_none=True,
    dependencies=[Depends(require_sspanel_api_key)],
)
async def get_job(job_id: str, store: SspanelExecutorStore = Depends(get_store)) -> JobProgressResponse:
    job = store.find_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="SS-panel executor job not found")
    return JobProgressResponse(
        jobId=job["jobId"],
        status=job["status"],
        completedCount=job.get("completedCount", 0),
        totalCount=job.get("totalCount", 0),
        errorCode=job.get("errorCode"),
        errorMessage=job.get("errorMessage"),
        result=job.get("result") or {},
        accountHealth=job.get("accountHealth") or {},
    )


@router.post(
    "/jobs/{job_id}/cancel",
    response_model=JobProgressResponse,
    response_model_exclude_none=True,
    dependencies=[Depends(require_sspanel_api_key)],
)
async def cancel_job(job_id: str, store: SspanelExecutorStore = Depends(get_store)) -> JobProgressResponse:
    job = store.cancel_job(job_id)
    return JobProgressResponse(
        jobId=job["jobId"],
        status=job["status"],
        completedCount=job.get("completedCount", 0),
        totalCount=job.get("totalCount", 0),
        result=job.get("result") or {},
        accountHealth=job.get("accountHealth") or {},
    )


@router.post(
    "/accounts/import-session",
    response_model=ImportSessionResponse,
    dependencies=[Depends(require_sspanel_api_key)],
)
async def import_session(
    request: ImportSessionRequest,
    store: SspanelExecutorStore = Depends(get_store),
) -> ImportSessionResponse:
    account = store.import_account(request)
    return ImportSessionResponse(executorAccountId=account["executorAccountId"], status=account["status"])


@router.get(
    "/accounts/{executor_account_id}/health",
    dependencies=[Depends(require_sspanel_api_key)],
)
async def get_account_health(
    executor_account_id: str,
    store: SspanelExecutorStore = Depends(get_store),
) -> dict[str, Any]:
    account = store.get_account(executor_account_id)
    if not account:
        raise HTTPException(status_code=404, detail="SS-panel executor account not found")
    return {
        "executorAccountId": executor_account_id,
        "status": account.get("status", "unknown"),
        "lastCheckedAt": _utc_now(),
    }
