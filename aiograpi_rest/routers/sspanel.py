import asyncio
import contextlib
import hashlib
import hmac
import ipaddress
import json
import logging
import math
import os
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Optional
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import requests
from aiograpi import Client
from aiograpi import exceptions as aiograpi_exceptions
from fastapi import APIRouter, Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from aiograpi_rest.sspanel_storage import Query, SQLiteJsonStore, StorageConfigurationError, read_secret

logger = logging.getLogger("aiograpi_rest.sspanel")

router = APIRouter(
    prefix="/module/v1",
    tags=["SS-panel Module API"],
    responses={404: {"description": "Not found"}},
)

MODULE_ID = "instagram-executor"
CONTRACT_PATH = Path(__file__).resolve().parents[1] / "sspanel_contract.json"
CONTRACT_FIXTURE = json.loads(CONTRACT_PATH.read_text())
CONTRACT_VERSIONS = tuple(CONTRACT_FIXTURE["contractVersions"])
CONTRACT_FEATURES = tuple(CONTRACT_FIXTURE.get("moduleFeatures", []))
IMPLEMENTED_CAPABILITIES = frozenset(CONTRACT_FIXTURE["instagramImplementedCapabilities"])
IMPLEMENTED_WORKFLOW_TYPES = tuple(CONTRACT_FIXTURE.get("workflowTypes", []))

CAPABILITY_INPUT_SCHEMAS: dict[str, dict[str, Any]] = {
    "instagram.account.health": {"type": "object", "properties": {}},
    "instagram.profile.get": {
        "type": "object",
        "properties": {
            "username": {"type": "string", "title": "Username"},
            "userId": {"type": "string", "title": "User ID"},
        },
    },
    "instagram.comments.list": {
        "type": "object",
        "properties": {
            "mediaId": {"type": "string", "title": "Media ID"},
            "amount": {"type": "integer", "title": "Amount", "default": 20},
            "cursor": {"type": "string", "title": "Cursor"},
        },
    },
    "instagram.comments.reply": {
        "type": "object",
        "properties": {
            "mediaId": {"type": "string", "title": "Media ID"},
            "commentId": {"type": "string", "title": "Comment ID"},
            "text": {"type": "string", "title": "Reply text"},
        },
    },
    "instagram.comments.smart_reply": {
        "type": "object",
        "properties": {
            "mediaIds": {"type": "array", "title": "Media IDs", "items": {"type": "string"}},
            "maxCandidates": {"type": "integer", "title": "Maximum candidates", "default": 1},
        },
    },
    "instagram.comments.delete": {
        "type": "object",
        "properties": {
            "mediaId": {"type": "string", "title": "Media ID"},
            "commentIds": {"type": "array", "title": "Comment IDs", "items": {"type": "string"}},
        },
    },
    "instagram.comments.pin": {
        "type": "object",
        "properties": {
            "mediaId": {"type": "string", "title": "Media ID"},
            "commentIds": {"type": "array", "title": "Comment IDs", "items": {"type": "string"}},
        },
    },
    "instagram.media.upload.photo": {
        "type": "object",
        "properties": {
            "mediaUrl": {"type": "string", "title": "Media URL"},
            "caption": {"type": "string", "title": "Caption"},
        },
    },
    "instagram.media.upload.video": {
        "type": "object",
        "properties": {
            "mediaUrl": {"type": "string", "title": "Media URL"},
            "caption": {"type": "string", "title": "Caption"},
            "thumbnailUrl": {"type": "string", "title": "Thumbnail URL"},
        },
    },
    "instagram.media.upload.reel": {
        "type": "object",
        "properties": {
            "mediaUrl": {"type": "string", "title": "Media URL"},
            "caption": {"type": "string", "title": "Caption"},
            "thumbnailUrl": {"type": "string", "title": "Thumbnail URL"},
        },
    },
    "instagram.story.upload": {
        "type": "object",
        "properties": {
            "mediaUrl": {"type": "string", "title": "Media URL"},
            "mediaType": {"type": "string", "title": "Media type", "enum": ["photo", "video"]},
        },
    },
    "instagram.dm.inbox": {
        "type": "object",
        "properties": {"amount": {"type": "integer", "title": "Amount", "default": 20}},
    },
    "instagram.dm.send": {
        "type": "object",
        "properties": {
            "userId": {"type": "string", "title": "User ID"},
            "text": {"type": "string", "title": "Message text"},
        },
    },
    "instagram.dm.reply": {
        "type": "object",
        "properties": {
            "threadId": {"type": "string", "title": "Thread ID"},
            "text": {"type": "string", "title": "Message text"},
        },
    },
    "instagram.insights.basic": {
        "type": "object",
        "properties": {
            "scope": {"type": "string", "title": "Scope", "enum": ["account", "media"]},
            "mediaId": {"type": "string", "title": "Media ID"},
        },
    },
    "instagram.warmup": {
        "type": "object",
        "properties": {
            "actionMix": {"type": "array", "title": "Action mix", "items": {"type": "string"}},
            "targetIds": {"type": "array", "title": "Target IDs", "items": {"type": "string"}},
        },
    },
}

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
    "instagram.warmup",
    "instagram.comments.reply",
    "instagram.comments.smart_reply",
    "instagram.comments.delete",
    "instagram.comments.pin",
    "instagram.dm.inbox",
    "instagram.dm.send",
    "instagram.dm.reply",
    "instagram.insights.basic",
]
EventType = Literal[
    "job.accepted",
    "job.started",
    "job.progressed",
    "workflow.paused",
    "action.completed",
    "action.failed",
    "account.health_changed",
    "job.completed",
    "job.partially_completed",
    "job.rate_limited",
    "job.failed",
    "job.cancelled",
]

WRITE_ACTIONS = {
    "instagram.media.upload.photo",
    "instagram.media.upload.video",
    "instagram.media.upload.reel",
    "instagram.story.upload",
    "instagram.warmup",
    "instagram.comments.reply",
    "instagram.comments.smart_reply",
    "instagram.comments.delete",
    "instagram.comments.pin",
    "instagram.dm.send",
    "instagram.dm.reply",
}
ACTIVE_WRITE_STATUSES = {"queued", "running", "paused", "rate_limited"}
TERMINAL_STATUSES = {
    "completed",
    "partially_completed",
    "failed",
    "cancelled",
    "challenge_required",
    "session_expired",
    "banned_or_locked",
}
SENSITIVE_KEY_PARTS = ("password", "session", "cookie", "proxy", "token", "secret")

_store: Optional["SspanelExecutorStore"] = None
_worker: Optional["ExecutorWorker"] = None
sspanel_executor_key_header = APIKeyHeader(
    name="X-SSPanel-Executor-Key",
    scheme_name="SspanelExecutorKey",
    description="SS-panel executor API key for /sspanel/* facade routes.",
    auto_error=False,
)
sspanel_executor_key_id_header = APIKeyHeader(
    name="X-SSPanel-Executor-Key-Id",
    scheme_name="SspanelExecutorKeyId",
    description="Optional key identifier used during executor credential rotation.",
    auto_error=False,
)

EXECUTOR_SCOPES = {
    "manifest:read",
    "jobs:read",
    "jobs:write",
    "accounts:read",
    "accounts:write",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _store_path() -> str:
    explicit = os.getenv("SSPANEL_EXECUTOR_DB_PATH")
    if explicit:
        return explicit
    aiograpi_db = os.getenv("AIOGRAPI_REST_DB_PATH")
    if aiograpi_db:
        return str(Path(aiograpi_db).with_name("sspanel-executor.sqlite3"))
    return "/data/sspanel-executor.sqlite3"


def _lease_ttl_seconds() -> int:
    raw = int(os.getenv("SSPANEL_ACCOUNT_LEASE_TTL_SECONDS", "120"))
    return max(15, min(raw, 3600))


def _worker_poll_interval_seconds() -> float:
    raw = float(os.getenv("SSPANEL_EXECUTOR_WORKER_POLL_SECONDS", "0.25"))
    return max(0.01, min(raw, 30.0))


def _worker_lease_ttl_seconds() -> int:
    raw = int(os.getenv("SSPANEL_EXECUTOR_WORKER_LEASE_TTL_SECONDS", "120"))
    return max(15, min(raw, 3600))


def _callback_secret() -> str:
    return _configured_secret("SSPANEL_CALLBACK_SECRET")


def _configured_secret(name: str) -> str:
    try:
        return read_secret(name)
    except StorageConfigurationError as error:
        raise HTTPException(status_code=503, detail="Executor secret configuration is invalid") from error


def _callback_url(value: Any) -> Optional[str]:
    candidate = str(value or os.getenv("SSPANEL_CALLBACK_URL", "")).strip()
    if not candidate or not _callback_secret():
        return None
    parsed = urlsplit(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        logger.warning("Ignoring invalid SS-panel callback URL")
        return None
    return candidate


def _callback_max_attempts() -> int:
    try:
        configured = int(os.getenv("SSPANEL_CALLBACK_MAX_ATTEMPTS", "12"))
    except ValueError:
        configured = 12
    return max(1, min(configured, 100))


def _callback_retry_delay(attempts: int) -> int:
    return min(900, max(5, 5 * (2 ** max(0, attempts - 1))))


def _executor_credentials() -> list[dict[str, Any]]:
    configured = _configured_secret("SSPANEL_EXECUTOR_API_KEYS")
    if configured:
        try:
            parsed = json.loads(configured)
        except json.JSONDecodeError as error:
            raise HTTPException(status_code=503, detail="Executor credential configuration is invalid") from error
        entries = parsed if isinstance(parsed, list) else [parsed]
        credentials: list[dict[str, Any]] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            key_id = entry.get("id")
            secret = entry.get("secret")
            scopes = entry.get("scopes", [])
            if not isinstance(key_id, str) or not key_id or not isinstance(secret, str) or not secret:
                continue
            normalized_scopes = [scope for scope in scopes if isinstance(scope, str) and scope in EXECUTOR_SCOPES]
            credentials.append({"id": key_id, "secret": secret, "scopes": normalized_scopes})
        if credentials:
            return credentials
        raise HTTPException(status_code=503, detail="Executor credential configuration is empty")

    legacy = _configured_secret("SSPANEL_EXECUTOR_API_KEY")
    return [{"id": "legacy", "secret": legacy, "scopes": ["*"]}] if legacy else []


def _authenticate_executor_key(key: Optional[str], key_id: Optional[str]) -> Optional[dict[str, Any]]:
    if not key:
        return None
    candidates = _executor_credentials()
    if key_id:
        candidates = [credential for credential in candidates if credential["id"] == key_id]
    for credential in candidates:
        if hmac.compare_digest(key, credential["secret"]):
            return credential
    return None


def _is_active_lease(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        expires_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return expires_at > datetime.now(timezone.utc)


def _is_due(value: Any) -> bool:
    if value is None:
        return True
    if not isinstance(value, str):
        return True
    try:
        scheduled_at = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return True
    return scheduled_at <= datetime.now(timezone.utc)


def _utc_after_seconds(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


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


def _usage_metric(action_type: str) -> Optional[str]:
    return {
        "instagram.comments.reply": "send_msg",
        "instagram.comments.smart_reply": "send_msg",
        "instagram.comments.delete": "message_deleted",
        "instagram.dm.send": "send_direct_msg",
        "instagram.dm.reply": "send_direct_msg",
        "instagram.media.upload.photo": "send_msg",
        "instagram.media.upload.video": "send_msg",
        "instagram.media.upload.reel": "send_msg",
        "instagram.story.upload": "send_msg",
    }.get(action_type)


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


def _result_identifier(value: Any, *keys: str) -> Optional[str]:
    if not isinstance(value, dict):
        return None
    for key in keys:
        candidate = value.get(key)
        if candidate is not None and str(candidate).strip():
            return str(candidate)
    return None


def _exception_status(error: Exception) -> JobStatus:
    if isinstance(
        error,
        (
            aiograpi_exceptions.ChallengeError,
            aiograpi_exceptions.CaptchaChallengeRequired,
            aiograpi_exceptions.CheckpointRequired,
        ),
    ):
        return "challenge_required"
    if isinstance(
        error,
        (
            aiograpi_exceptions.LoginRequired,
            aiograpi_exceptions.ClientLoginRequired,
            aiograpi_exceptions.PreLoginRequired,
            aiograpi_exceptions.ReloginAttemptExceeded,
            aiograpi_exceptions.ClientUnauthorizedError,
        ),
    ):
        return "session_expired"
    if isinstance(
        error,
        (
            aiograpi_exceptions.AccountSuspended,
            aiograpi_exceptions.SentryBlock,
        ),
    ):
        return "banned_or_locked"
    if isinstance(
        error,
        (
            aiograpi_exceptions.FeedbackRequired,
            aiograpi_exceptions.PleaseWaitFewMinutes,
            aiograpi_exceptions.RateLimitError,
        ),
    ):
        return "rate_limited"

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


def _safe_error_message(error: Exception) -> str:
    message = str(error)
    if any(part in message.lower() for part in ("password", "sessionid", "cookie", "proxy", "token", "secret")):
        return "External Instagram operation failed with a sensitive provider error"
    return message[:500]


def _media_max_bytes() -> int:
    try:
        configured = int(os.getenv("SSPANEL_MEDIA_MAX_BYTES", str(50 * 1024 * 1024)))
    except ValueError:
        configured = 50 * 1024 * 1024
    return max(1 * 1024 * 1024, min(configured, 500 * 1024 * 1024))


def _validate_external_media_url(url: str) -> str:
    parsed = urlsplit(url.strip())
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme not in {"http", "https"} or not hostname or parsed.username or parsed.password:
        raise ValueError("media URL must be an HTTP(S) URL without embedded credentials")
    if hostname in {"localhost", "localhost.localdomain"}:
        raise ValueError("media URL host is not publicly reachable")
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        address = None
    if address and (address.is_private or address.is_loopback or address.is_link_local or address.is_reserved or address.is_unspecified):
        raise ValueError("media URL host is not publicly reachable")
    return url.strip()


def _download_media_to_temp(url: str, default_suffix: str) -> Path:
    """Fetch one approved media URL into executor-only temporary storage."""
    url = _validate_external_media_url(url)
    parsed = urlsplit(url)
    suffix = Path(parsed.path).suffix.lower()
    if len(suffix) > 10 or not suffix.replace(".", "").isalnum():
        suffix = default_suffix
    temporary_path: Optional[Path] = None
    response = None
    try:
        response = requests.get(url, stream=True, timeout=30, allow_redirects=True)
        response.raise_for_status()
        content_length = response.headers.get("content-length") if getattr(response, "headers", None) else None
        if content_length and int(content_length) > _media_max_bytes():
            raise ValueError("media file exceeds executor size limit")
        with tempfile.NamedTemporaryFile(prefix="sspanel-media-", suffix=suffix, delete=False) as stream:
            temporary_path = Path(stream.name)
            total = 0
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if total > _media_max_bytes():
                    raise ValueError("media file exceeds executor size limit")
                stream.write(chunk)
        return temporary_path
    except Exception:
        if temporary_path:
            temporary_path.unlink(missing_ok=True)
        raise
    finally:
        if response is not None and hasattr(response, "close"):
            response.close()


def _remove_temp_path(path: Optional[Path]) -> None:
    if path:
        path.unlink(missing_ok=True)


def create_instagram_health_client(account: dict[str, Any]) -> Client:
    client = Client()
    settings = account.get("settings")
    if settings:
        client.set_settings(settings)
    proxy = account.get("proxy")
    if proxy:
        client.set_proxy(proxy)
    return client


def create_instagram_profile_client(account: dict[str, Any]) -> Client:
    """Build the platform client for a read-only profile capability."""
    return create_instagram_health_client(account)


def create_instagram_comments_client(account: dict[str, Any]) -> Client:
    """Build the platform client for comment capabilities."""
    return create_instagram_health_client(account)


def create_instagram_media_client(account: dict[str, Any]) -> Client:
    """Build the platform client for media and story capabilities."""
    return create_instagram_health_client(account)


def create_instagram_direct_client(account: dict[str, Any]) -> Client:
    """Build the platform client for direct-message capabilities."""
    return create_instagram_health_client(account)


def create_instagram_insights_client(account: dict[str, Any]) -> Client:
    """Build the platform client for insights capabilities."""
    return create_instagram_health_client(account)


class AccountSelector(BaseModel):
    mode: Literal["system", "specific"]
    accountIds: Optional[list[str]] = None
    constraints: Optional[dict[str, Any]] = None


class ExecutorActivityWindow(BaseModel):
    from_: str = Field(..., alias="from", min_length=1)
    to: str = Field(..., min_length=1)
    timezone: str = Field(..., min_length=1)

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("from_", "to")
    @classmethod
    def validate_clock_time(cls, value: str) -> str:
        try:
            datetime.strptime(value, "%H:%M")
        except ValueError as error:
            raise ValueError("activity window time must use HH:MM") from error
        return value

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except Exception as error:
            raise ValueError("activity window timezone must be an IANA timezone") from error
        return value


class ExecutorActionLimit(BaseModel):
    hourly: Optional[int] = Field(default=None, ge=0)
    daily: Optional[int] = Field(default=None, ge=0)

    model_config = ConfigDict(extra="forbid")


class ExecutorProgressiveLimitPolicy(BaseModel):
    driver: Literal["calendar_days", "success_count", "hybrid"]
    startPercent: float = Field(..., ge=0, le=100)
    targetPercent: float = Field(..., ge=0, le=100)
    rampDays: Optional[float] = Field(default=None, gt=0)
    rampSuccesses: Optional[int] = Field(default=None, gt=0)
    hybridPolicy: Optional[Literal["min"]] = None

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_ramp(self) -> "ExecutorProgressiveLimitPolicy":
        if self.startPercent > self.targetPercent:
            raise ValueError("progressive startPercent must not exceed targetPercent")
        if self.driver in {"calendar_days", "hybrid"} and self.rampDays is None:
            raise ValueError("rampDays is required for calendar_days and hybrid policies")
        if self.driver in {"success_count", "hybrid"} and self.rampSuccesses is None:
            raise ValueError("rampSuccesses is required for success_count and hybrid policies")
        return self


class ExecutorPolicyEnvelope(BaseModel):
    policyVersion: str = Field(..., min_length=1)
    activityWindows: Optional[list[ExecutorActivityWindow]] = None
    actionLimits: Optional[dict[str, ExecutorActionLimit]] = None
    durationMinutes: Optional[int] = Field(default=None, gt=0)
    progressiveLimits: Optional[ExecutorProgressiveLimitPolicy] = None
    riskProfile: Optional[Literal["safe", "standard", "fast"]] = None
    scenarioRef: Optional[str] = None
    targetPolicy: Optional[dict[str, Any]] = None

    model_config = ConfigDict(extra="forbid")


def _validate_http_media_url(value: str) -> str:
    return _validate_external_media_url(value)


class MediaUploadPayload(BaseModel):
    mediaUrl: str = Field(..., min_length=1)
    caption: str = Field(default="", max_length=2200)
    thumbnailUrl: Optional[str] = Field(default=None, min_length=1)

    model_config = ConfigDict(extra="forbid")

    _validate_media_url = field_validator("mediaUrl", "thumbnailUrl")(_validate_http_media_url)


class StoryUploadPayload(MediaUploadPayload):
    mediaType: Literal["photo", "video"]


class CommentModerationPayload(BaseModel):
    mediaId: str = Field(..., min_length=1)
    commentIds: list[str | int] = Field(..., min_length=1, max_length=100)

    model_config = ConfigDict(extra="forbid")

    @field_validator("commentIds")
    @classmethod
    def validate_comment_ids(cls, value: list[str | int]) -> list[str | int]:
        if any(isinstance(item, bool) or not str(item).isdigit() for item in value):
            raise ValueError("commentIds must contain numeric IDs")
        return value


class DmInboxPayload(BaseModel):
    amount: int = Field(default=20, ge=1, le=100)
    selectedFilter: Optional[Literal["flagged", "unread"]] = None
    box: Optional[Literal["primary", "general"]] = None
    threadMessageLimit: Optional[int] = Field(default=None, ge=1, le=50)

    model_config = ConfigDict(extra="forbid")


class DmSendPayload(BaseModel):
    text: str = Field(..., min_length=1, max_length=10000)
    userIds: Optional[list[str | int]] = Field(default=None, min_length=1, max_length=100)
    threadIds: Optional[list[str | int]] = Field(default=None, min_length=1, max_length=100)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_targets(self) -> "DmSendPayload":
        if bool(self.userIds) == bool(self.threadIds):
            raise ValueError("exactly one of userIds or threadIds is required")
        targets = self.userIds or self.threadIds or []
        if any(isinstance(item, bool) or not str(item).isdigit() for item in targets):
            raise ValueError("DM target IDs must be numeric")
        return self


class DmReplyPayload(BaseModel):
    threadId: str | int
    text: str = Field(..., min_length=1, max_length=10000)

    model_config = ConfigDict(extra="forbid")

    @field_validator("threadId")
    @classmethod
    def validate_thread_id(cls, value: str | int) -> str | int:
        if isinstance(value, bool) or not str(value).isdigit():
            raise ValueError("threadId must be numeric")
        return value


class InsightsPayload(BaseModel):
    mediaId: Optional[str] = Field(default=None, min_length=1)

    model_config = ConfigDict(extra="forbid")


class JobStartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotencyKey: str = Field(..., min_length=1)
    orderId: int
    platform: Platform
    actionType: ActionType
    quantity: int = Field(..., ge=1)
    accountSelector: AccountSelector
    payload: dict[str, Any] = Field(default_factory=dict)
    policyEnvelope: Optional[ExecutorPolicyEnvelope] = None
    callbackUrl: Optional[str] = None

    @model_validator(mode="after")
    def validate_typed_payload(self) -> "JobStartRequest":
        validators = {
            "instagram.media.upload.photo": MediaUploadPayload,
            "instagram.media.upload.video": MediaUploadPayload,
            "instagram.media.upload.reel": MediaUploadPayload,
            "instagram.story.upload": StoryUploadPayload,
            "instagram.comments.delete": CommentModerationPayload,
            "instagram.comments.pin": CommentModerationPayload,
            "instagram.dm.inbox": DmInboxPayload,
            "instagram.dm.send": DmSendPayload,
            "instagram.dm.reply": DmReplyPayload,
            "instagram.insights.basic": InsightsPayload,
        }
        payload_model = validators.get(self.actionType)
        if payload_model:
            payload_model.model_validate(self.payload)
        if self.actionType in {
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
        } and self.quantity != 1:
            raise ValueError("this Instagram capability accepts quantity=1")
        return self

    @field_validator("callbackUrl")
    @classmethod
    def validate_callback_url(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        parsed = urlsplit(value.strip())
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
            raise ValueError("callbackUrl must be an HTTP(S) URL without embedded credentials")
        return value.strip()


class JobStartResponse(BaseModel):
    jobId: str
    status: JobStatus
    acceptedAt: str


class ExecutorUsageEvent(BaseModel):
    metric: str
    amount: float
    unit: Optional[str] = None
    window: Optional[str] = None


class ExecutorActionEvent(BaseModel):
    eventId: str
    sequence: int
    jobId: str
    eventType: EventType
    occurredAt: str
    actionType: Optional[ActionType] = None
    status: Optional[JobStatus] = None
    executorAccountId: Optional[str] = None
    targetRef: Optional[str] = None
    quantity: Optional[int] = None
    completedCount: Optional[int] = None
    totalCount: Optional[int] = None
    errorCode: Optional[str] = None
    errorMessage: Optional[str] = None
    usage: Optional[ExecutorUsageEvent] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class JobProgressResponse(BaseModel):
    jobId: str
    status: JobStatus
    completedCount: int
    totalCount: int
    nextRunAt: Optional[str] = None
    assignedAccountIds: Optional[list[str]] = None
    errorCode: Optional[str] = None
    errorMessage: Optional[str] = None
    result: dict[str, Any] = Field(default_factory=dict)
    accountHealth: dict[str, Any] = Field(default_factory=dict)
    eventSequence: Optional[int] = None
    events: Optional[list[ExecutorActionEvent]] = None


class ContentDecision(BaseModel):
    candidateId: str = Field(..., min_length=1)
    text: str = ""
    decision: Literal["publish", "skip"]
    reasonCode: Optional[str] = None


class WorkflowInputRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    inputIdempotencyKey: str = Field(..., min_length=1)
    decisions: list[ContentDecision] = Field(default_factory=list)


class ModuleManifestResponse(BaseModel):
    moduleId: str
    platform: Platform
    contractVersions: list[str]
    features: list[str]
    capabilities: list[ActionType]
    inputSchemas: dict[str, dict[str, Any]]
    workflowTypes: list[str]
    supportsPolling: bool
    supportsCallbacks: bool


class ModuleHealthResponse(BaseModel):
    moduleId: str
    status: Literal["ok", "degraded", "unavailable"]


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
        self.db = SQLiteJsonStore(str(db_path))
        self.jobs = self.db.table("jobs")
        self.accounts = self.db.table("accounts")
        self.usage = self.db.table("usage")
        self.outbox = self.db.table("outbox")

    def close(self) -> None:
        self.db.close()

    def find_job_by_idempotency(self, idempotency_key: str) -> Optional[dict[str, Any]]:
        rows = self.jobs.search(Query().idempotencyKey == idempotency_key)
        return rows[0] if rows else None

    def find_job(self, job_id: str) -> Optional[dict[str, Any]]:
        rows = self.jobs.search(Query().jobId == job_id)
        return rows[0] if rows else None

    def create_job(self, request: JobStartRequest) -> dict[str, Any]:
        job, _ = self.get_or_create_job(request)
        return job

    def _build_job(self, request: JobStartRequest) -> dict[str, Any]:
        accepted_at = _utc_now()
        return {
            "jobId": _stable_id("ig_job", request.idempotencyKey),
            "idempotencyKey": request.idempotencyKey,
            "orderId": request.orderId,
            "platform": request.platform,
            "actionType": request.actionType,
            "quantity": request.quantity,
            "accountSelector": request.accountSelector.model_dump(exclude_none=True),
            "assignedAccountIds": (request.accountSelector.accountIds or None)
            if request.accountSelector.mode == "specific"
            else None,
            "payload": request.payload,
            "policyEnvelope": request.policyEnvelope.model_dump(by_alias=True, exclude_none=True)
            if request.policyEnvelope
            else None,
            "callbackUrl": request.callbackUrl,
            "status": "queued",
            "completedCount": 0,
            "totalCount": request.quantity,
            "result": {},
            "accountHealth": {},
            "attempts": 0,
            "nextRunAt": accepted_at,
            "acceptedAt": accepted_at,
            "updatedAt": accepted_at,
        }

    def get_or_create_job(self, request: JobStartRequest) -> tuple[dict[str, Any], bool]:
        with self.db.transaction():
            existing = self.find_job_by_idempotency(request.idempotencyKey)
            if existing:
                return existing, False

            job = self._build_job(request)
            self.jobs.insert_in_transaction(job)
            self._assert_write_capacity(request, job["jobId"], in_transaction=True)

        self.append_event(
            job["jobId"],
            "job.accepted",
            action_type=request.actionType,
            status="queued",
            quantity=request.quantity,
            total_count=request.quantity,
        )
        return self.find_job(job["jobId"]) or job, True

    def update_job(self, job_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        job = self.find_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="SS-panel executor job not found")
        if job.get("status") == "cancelled" and updates.get("status") not in {None, "cancelled"}:
            return job
        is_operator_resume = (
            updates.get("status") == "queued"
            and updates.get("pauseReason") is None
        )
        if (
            job.get("status") == "paused"
            and job.get("pauseReason") == "operator"
            and updates.get("status") not in {None, "paused"}
            and not is_operator_resume
        ):
            return job
        updated = {**job, **updates, "updatedAt": _utc_now()}
        self.jobs.update(updated, Query().jobId == job_id)
        if updated.get("status") in TERMINAL_STATUSES:
            self.release_account_leases(job_id)
        if updated.get("status") != job.get("status"):
            event_type = {
                "paused": "workflow.paused",
                "completed": "job.completed",
                "partially_completed": "job.partially_completed",
                "rate_limited": "job.rate_limited",
                "cancelled": "job.cancelled",
            }.get(updated.get("status"), "job.failed" if updated.get("status") in TERMINAL_STATUSES else "job.progressed")
            self.append_event(
                job_id,
                event_type,
                action_type=updated.get("actionType"),
                status=updated.get("status"),
                completed_count=updated.get("completedCount", 0),
                total_count=updated.get("totalCount", 0),
                error_code=updated.get("errorCode"),
                error_message=updated.get("errorMessage"),
                metadata={"phase": (updated.get("result") or {}).get("phase")}
                if isinstance(updated.get("result"), dict) and (updated.get("result") or {}).get("phase")
                else {},
            )
            updated = self.find_job(job_id) or updated
        return updated

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        job = self.find_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="SS-panel executor job not found")
        if job.get("status") in TERMINAL_STATUSES:
            return job
        cancelled = {
            **job,
            "status": "cancelled",
            "cancelRequested": True,
            "cancelRequestedAt": _utc_now(),
            "workerLeaseExpiresAt": None,
            "updatedAt": _utc_now(),
        }
        self.jobs.update(cancelled, Query().jobId == job_id)
        self.release_account_leases(job_id)
        self.append_event(job_id, "job.cancelled", action_type=job.get("actionType"), status="cancelled")
        return self.find_job(job_id) or cancelled

    def pause_job(self, job_id: str) -> dict[str, Any]:
        job = self.find_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="SS-panel executor job not found")
        if job.get("status") in TERMINAL_STATUSES:
            return job
        if job.get("status") == "paused" and job.get("pauseReason") == "operator":
            return job
        paused = self.update_job(
            job_id,
            {
                "status": "paused",
                "pauseReason": "operator",
                "pauseRequestedAt": _utc_now(),
                "nextRunAt": None,
            },
        )
        self.release_account_leases(job_id)
        return paused

    def resume_job(self, job_id: str, confirm_provider_retry: bool = False) -> dict[str, Any]:
        job = self.find_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="SS-panel executor job not found")
        if job.get("status") != "paused" or job.get("pauseReason") != "operator":
            if job.get("pauseReason") == "write_reconciliation_required" and not confirm_provider_retry:
                raise HTTPException(
                    status_code=409,
                    detail="Provider write outcome is unknown; explicit confirmProviderRetry is required",
                )
            if job.get("pauseReason") == "write_reconciliation_required" and confirm_provider_retry:
                return self.update_job(
                    job_id,
                    {
                        "status": "queued",
                        "pauseReason": None,
                        "providerCallStartedAt": None,
                        "errorCode": None,
                        "errorMessage": None,
                        "nextRunAt": _utc_now(),
                    },
                )
            return job
        return self.update_job(
            job_id,
            {
                "status": "queued",
                "pauseReason": None,
                "pauseRequestedAt": None,
                "nextRunAt": _utc_now(),
            },
        )

    def append_event(
        self,
        job_id: str,
        event_type: EventType,
        *,
        action_type: Optional[str] = None,
        status: Optional[str] = None,
        executor_account_id: Optional[str] = None,
        target_ref: Optional[str] = None,
        quantity: Optional[int] = None,
        completed_count: Optional[int] = None,
        total_count: Optional[int] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
        usage: Optional[dict[str, Any]] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        with self.db.transaction():
            job = self.find_job(job_id)
            if not job:
                raise HTTPException(status_code=404, detail="SS-panel executor job not found")
            sequence = int(job.get("eventSequence", 0)) + 1
            event = {
                "eventId": f"{job_id}:event:{sequence}",
                "sequence": sequence,
                "jobId": job_id,
                "eventType": event_type,
                "occurredAt": _utc_now(),
                "actionType": action_type,
                "status": status,
                "executorAccountId": executor_account_id,
                "targetRef": target_ref,
                "quantity": quantity,
                "completedCount": completed_count,
                "totalCount": total_count,
                "errorCode": error_code,
                "errorMessage": _safe_error_message(RuntimeError(error_message)) if error_message else None,
                "usage": usage,
                "metadata": _redact(metadata or {}),
            }
            compact_event = {key: value for key, value in event.items() if value is not None}
            events = [*job.get("events", []), compact_event]
            now = _utc_now()
            self.jobs.update_in_transaction(
                {"events": events, "eventSequence": sequence, "updatedAt": now},
                Query().jobId == job_id,
            )
            callback_url = _callback_url(job.get("callbackUrl"))
            if callback_url:
                self.outbox.insert_in_transaction({
                    "outboxId": f"{job_id}:{event['eventId']}",
                    "jobId": job_id,
                    "eventId": event["eventId"],
                    "callbackUrl": callback_url,
                    "payload": {
                        "contractVersion": CONTRACT_VERSIONS[0],
                        "moduleId": MODULE_ID,
                        "event": compact_event,
                    },
                    "status": "queued",
                    "attempts": 0,
                    "nextAttemptAt": now,
                    "createdAt": now,
                    "updatedAt": now,
                })
            return event

    def claim_outbox(self, worker_id: str) -> Optional[dict[str, Any]]:
        with self.db.transaction():
            now = _utc_now()
            candidates = [
                row for row in self.outbox.all()
                if row.get("status") == "queued" and _is_due(row.get("nextAttemptAt"))
            ]
            candidates.sort(key=lambda row: (row.get("nextAttemptAt", ""), row.get("outboxId", "")))
            if not candidates:
                return None
            row = candidates[0]
            claimed = {
                **row,
                "status": "delivering",
                "workerId": worker_id,
                "workerLeaseExpiresAt": _utc_after_seconds(_worker_lease_ttl_seconds()),
                "attempts": int(row.get("attempts", 0)) + 1,
                "updatedAt": now,
            }
            self.outbox.update_in_transaction(claimed, Query().outboxId == row["outboxId"])
            return claimed

    def mark_outbox_delivered(self, outbox_id: str) -> None:
        self.outbox.update(
            {
                "status": "delivered",
                "deliveredAt": _utc_now(),
                "workerId": None,
                "workerLeaseExpiresAt": None,
                "updatedAt": _utc_now(),
            },
            Query().outboxId == outbox_id,
        )

    def mark_outbox_failed(self, row: dict[str, Any], error_message: str) -> None:
        attempts = int(row.get("attempts", 0))
        terminal = attempts >= _callback_max_attempts()
        self.outbox.update(
            {
                "status": "failed" if terminal else "queued",
                "lastError": _safe_error_message(RuntimeError(error_message)),
                "nextAttemptAt": None if terminal else _utc_after_seconds(_callback_retry_delay(attempts)),
                "workerId": None,
                "workerLeaseExpiresAt": None,
                "updatedAt": _utc_now(),
            },
            Query().outboxId == row["outboxId"],
        )

    def reserve_policy_capacity(self, request: JobStartRequest, job: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Reserve the requested quantity against the immutable policy snapshot."""
        policy = request.policyEnvelope
        account_ids = sorted(set(request.accountSelector.accountIds or []))
        if not policy or not policy.actionLimits or not account_ids:
            return None
        limit = policy.actionLimits.get(request.actionType) or policy.actionLimits.get("*")
        if not limit:
            return None

        now = datetime.now(timezone.utc)
        requested_amount = 1 if request.actionType == "instagram.warmup" else request.quantity
        multiplier = self._progressive_limit_multiplier(request.policyEnvelope, job, now)
        windows: list[tuple[str, int, str, str]] = []
        if limit.hourly is not None:
            maximum = self._effective_limit(limit.hourly, multiplier)
            windows.append(("hourly", maximum, now.strftime("%Y-%m-%dT%H"), "hour"))
        if limit.daily is not None:
            maximum = self._effective_limit(limit.daily, multiplier)
            windows.append(("daily", maximum, now.strftime("%Y-%m-%d"), "day"))
        if not windows:
            return None

        reservations: list[dict[str, Any]] = []
        for account_id in account_ids:
            for window, maximum, window_start, unit in windows:
                usage_key = f"{account_id}:{request.actionType}:{window}:{window_start}"
                row = self.usage.get(Query().usageKey == usage_key) or {"used": 0}
                used = int(row.get("used", 0))
                if used + requested_amount > maximum:
                    next_run = now + timedelta(hours=1 if unit == "hour" else 24)
                    return {
                        "code": "POLICY_LIMIT_REACHED",
                        "message": f"{window} policy limit reached for {request.actionType}",
                        "nextRunAt": next_run.isoformat().replace("+00:00", "Z"),
                    }
                reservations.append({
                    "usageKey": usage_key,
                    "accountId": account_id,
                    "actionType": request.actionType,
                    "window": window,
                    "windowStart": window_start,
                    "amount": requested_amount,
                    "maximum": maximum,
                })

        for reservation in reservations:
            self.usage.upsert(
                {
                    **reservation,
                    "used": int((self.usage.get(Query().usageKey == reservation["usageKey"]) or {}).get("used", 0))
                    + requested_amount,
                    "updatedAt": _utc_now(),
                },
                Query().usageKey == reservation["usageKey"],
            )
        return {"reservations": reservations}

    @staticmethod
    def _effective_limit(maximum: int, multiplier: float) -> int:
        if maximum <= 0 or multiplier <= 0:
            return 0
        return max(1, math.ceil(maximum * multiplier))

    @staticmethod
    def _progressive_limit_multiplier(
        policy: Optional[ExecutorPolicyEnvelope],
        job: dict[str, Any],
        now: datetime,
    ) -> float:
        progressive = policy.progressiveLimits if policy else None
        if not progressive:
            return 1.0
        accepted_at = job.get("acceptedAt")
        try:
            started_at = datetime.fromisoformat(str(accepted_at).replace("Z", "+00:00"))
        except ValueError:
            started_at = now
        calendar_ratio = 1.0
        if progressive.rampDays:
            calendar_ratio = min(1.0, max(0.0, (now - started_at).total_seconds() / 86400 / progressive.rampDays))
        success_ratio = 1.0
        if progressive.rampSuccesses:
            success_ratio = min(1.0, max(0.0, int(job.get("completedCount", 0)) / progressive.rampSuccesses))
        if progressive.driver == "calendar_days":
            ratio = calendar_ratio
        elif progressive.driver == "success_count":
            ratio = success_ratio
        else:
            ratio = min(calendar_ratio, success_ratio) if progressive.hybridPolicy == "min" else max(calendar_ratio, success_ratio)
        percent = progressive.startPercent + (progressive.targetPercent - progressive.startPercent) * ratio
        return max(0.0, min(1.0, percent / 100))

    def activity_window_block(self, request: JobStartRequest) -> Optional[dict[str, Any]]:
        policy = request.policyEnvelope
        if not policy or not policy.activityWindows:
            return None
        now = datetime.now(timezone.utc)
        next_start: Optional[datetime] = None
        for window in policy.activityWindows:
            local_now = now.astimezone(ZoneInfo(window.timezone))
            start = datetime.strptime(window.from_, "%H:%M").time()
            end = datetime.strptime(window.to, "%H:%M").time()
            current = local_now.time()
            active = (
                True if start == end else
                start <= current < end if start < end else
                current >= start or current < end
            )
            candidate_date = local_now.date()
            if not active:
                if current >= start and start < end:
                    candidate_date += timedelta(days=1)
                candidate = datetime.combine(candidate_date, start, tzinfo=ZoneInfo(window.timezone)).astimezone(timezone.utc)
                if candidate <= now:
                    candidate += timedelta(days=1)
                if next_start is None or candidate < next_start:
                    next_start = candidate
            else:
                return None
        return {
            "code": "POLICY_ACTIVITY_WINDOW_CLOSED",
            "message": "Instagram execution is outside the configured activity windows",
            "nextRunAt": (next_start or (now + timedelta(minutes=1))).isoformat().replace("+00:00", "Z"),
        }

    def recover_jobs(self, worker_id: str) -> int:
        """Return jobs owned by a previous worker to the durable queue."""
        recovered = 0
        for job in self.jobs.all():
            if job.get("status") != "running":
                continue
            if job.get("workerId") == worker_id and _is_active_lease(job.get("workerLeaseExpiresAt")):
                continue
            self.release_account_leases(job["jobId"])
            if job.get("providerCallStartedAt") and job.get("actionType") in WRITE_ACTIONS:
                self.jobs.update(
                    {
                        "status": "paused",
                        "pauseReason": "write_reconciliation_required",
                        "workerId": None,
                        "workerLeaseExpiresAt": None,
                        "nextRunAt": None,
                        "recoveredAt": _utc_now(),
                        "errorCode": "WRITE_RECONCILIATION_REQUIRED",
                        "errorMessage": "Provider write outcome is unknown after executor restart",
                        "updatedAt": _utc_now(),
                    },
                    Query().jobId == job["jobId"],
                )
                self.append_event(
                    job["jobId"],
                    "workflow.paused",
                    action_type=job.get("actionType"),
                    status="paused",
                    error_code="WRITE_RECONCILIATION_REQUIRED",
                    error_message="Provider write outcome is unknown after executor restart",
                )
                recovered += 1
                continue
            self.jobs.update(
                {
                    "status": "queued",
                    "workerId": None,
                    "workerLeaseExpiresAt": None,
                    "nextRunAt": _utc_now(),
                    "recoveredAt": _utc_now(),
                    "updatedAt": _utc_now(),
                },
                Query().jobId == job["jobId"],
            )
            recovered += 1
        return recovered

    def claim_next_job(self, worker_id: str) -> Optional[dict[str, Any]]:
        """Atomically claim one due queued job for this process."""
        candidates = [
            job
            for job in self.jobs.all()
            if job.get("status") == "queued" and _is_due(job.get("nextRunAt"))
        ]
        candidates.sort(key=lambda job: (job.get("acceptedAt", ""), job.get("jobId", "")))
        for job in candidates:
            if job.get("cancelRequested"):
                self.cancel_job(job["jobId"])
                continue
            try:
                request = request_from_job(job)
                if request.actionType in WRITE_ACTIONS and request.accountSelector.mode == "specific":
                    self.acquire_account_leases(
                        sorted(set(request.accountSelector.accountIds or [])),
                        job["jobId"],
                    )
            except HTTPException:
                continue

            policy_block = self.activity_window_block(request)
            if policy_block:
                self.update_job(
                    job["jobId"],
                    {
                        "status": "paused",
                        "errorCode": policy_block["code"],
                        "errorMessage": policy_block["message"],
                        "nextRunAt": policy_block["nextRunAt"],
                    },
                )
                self.release_account_leases(job["jobId"])
                continue

            policy_block = self.reserve_policy_capacity(request, job)
            if policy_block and policy_block.get("code") == "POLICY_LIMIT_REACHED":
                self.update_job(
                    job["jobId"],
                    {
                        "status": "paused",
                        "errorCode": policy_block["code"],
                        "errorMessage": policy_block["message"],
                        "nextRunAt": policy_block["nextRunAt"],
                    },
                )
                self.release_account_leases(job["jobId"])
                continue

            expiry = _utc_after_seconds(_worker_lease_ttl_seconds())
            claimed = {
                **job,
                "status": "running",
                "workerId": worker_id,
                "workerLeaseExpiresAt": expiry,
                "attempts": int(job.get("attempts", 0)) + 1,
                "startedAt": job.get("startedAt") or _utc_now(),
                "updatedAt": _utc_now(),
            }
            self.jobs.update(claimed, Query().jobId == job["jobId"])
            self.append_event(
                job["jobId"],
                "job.started",
                action_type=job.get("actionType"),
                status="running",
                quantity=job.get("quantity"),
                total_count=job.get("totalCount"),
                metadata={"policyReservation": policy_block} if policy_block else {},
            )
            return self.find_job(job["jobId"]) or claimed
        return None

    def renew_job_lease(self, job_id: str, worker_id: str) -> bool:
        job = self.find_job(job_id)
        if not job or job.get("status") != "running" or job.get("workerId") != worker_id:
            return False
        expiry = _utc_after_seconds(_worker_lease_ttl_seconds())
        self.jobs.update(
            {"workerLeaseExpiresAt": expiry, "updatedAt": _utc_now()},
            Query().jobId == job_id,
        )
        self._renew_account_leases(job_id)
        return True

    def _renew_account_leases(self, job_id: str) -> None:
        expiry = _utc_after_seconds(_lease_ttl_seconds())
        for account in self.accounts.search(Query().leaseJobId == job_id):
            self.accounts.update(
                {"leaseExpiresAt": expiry, "updatedAt": _utc_now()},
                Query().executorAccountId == account["executorAccountId"],
            )

    def accept_workflow_input(self, job_id: str, request: WorkflowInputRequest) -> dict[str, Any]:
        job = self.find_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="SS-panel executor job not found")
        if job.get("actionType") != "instagram.comments.smart_reply":
            raise HTTPException(status_code=422, detail="Job does not accept workflow input")
        if job.get("status") != "paused" or job.get("pauseReason") == "operator":
            if job.get("workflowInputIdempotencyKey") == request.inputIdempotencyKey:
                return job
            raise HTTPException(status_code=409, detail="Instagram workflow is not awaiting content")
        existing_input_key = job.get("workflowInputIdempotencyKey")
        if existing_input_key:
            if existing_input_key == request.inputIdempotencyKey:
                return job
            raise HTTPException(status_code=409, detail="Instagram workflow input already accepted")
        return self.update_job(
            job_id,
            {
                "status": "queued",
                "workflowInputIdempotencyKey": request.inputIdempotencyKey,
                "workflowInput": request.model_dump(),
                "nextRunAt": _utc_now(),
            },
        )

    def import_account(self, request: ImportSessionRequest) -> dict[str, Any]:
        identity = request.username or request.sessionid or json.dumps(request.settings or {}, sort_keys=True)
        if not identity:
            raise HTTPException(status_code=422, detail="username, sessionid, or settings is required")
        account_id = _stable_id("ig_acc", identity)
        existing = self.get_account(account_id) or {}
        row = {
            "executorAccountId": account_id,
            "username": request.username,
            "sessionid": request.sessionid,
            "settings": request.settings,
            "proxy": request.proxy,
            "status": "imported",
            "leaseJobId": existing.get("leaseJobId"),
            "leaseExpiresAt": existing.get("leaseExpiresAt"),
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

    def _assert_write_capacity(
        self,
        request: JobStartRequest,
        job_id: str,
        *,
        in_transaction: bool = False,
    ) -> None:
        if request.actionType not in WRITE_ACTIONS:
            return
        if request.accountSelector.mode != "specific":
            return
        account_ids = set(request.accountSelector.accountIds or [])
        if not account_ids:
            return
        if in_transaction:
            self._acquire_account_leases_in_transaction(sorted(account_ids), job_id)
        else:
            self.acquire_account_leases(sorted(account_ids), job_id)
        for row in self.jobs.all():
            if row.get("jobId") == job_id:
                continue
            if row.get("actionType") not in WRITE_ACTIONS:
                continue
            if row.get("status") not in ACTIVE_WRITE_STATUSES:
                continue
            row_selector = row.get("accountSelector") or {}
            row_account_ids = set(row_selector.get("accountIds") or [])
            if account_ids & row_account_ids:
                self.release_account_leases(job_id)
                raise HTTPException(status_code=409, detail="Instagram account already has an active write job")

    def acquire_account_leases(self, account_ids: list[str], job_id: str) -> None:
        with self.db.transaction():
            self._acquire_account_leases_in_transaction(account_ids, job_id)

    def _acquire_account_leases_in_transaction(self, account_ids: list[str], job_id: str) -> None:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=_lease_ttl_seconds())
        expiry = expires_at.isoformat().replace("+00:00", "Z")
        rows = self.accounts.rows_with_ids()
        by_account_id = {
            row.get("executorAccountId"): (row_id, row)
            for row_id, row in rows
        }
        updates: list[tuple[int, dict[str, Any]]] = []
        for account_id in account_ids:
            row_and_id = by_account_id.get(account_id)
            if not row_and_id:
                raise HTTPException(status_code=404, detail="SS-panel executor account not found")
            row_id, account = row_and_id
            current_job_id = account.get("leaseJobId")
            current_expiry = account.get("leaseExpiresAt")
            if current_job_id and current_job_id != job_id and _is_active_lease(current_expiry):
                raise HTTPException(status_code=409, detail="Instagram account lease is already held")
            updates.append((row_id, {**account, "leaseJobId": job_id, "leaseExpiresAt": expiry, "updatedAt": _utc_now()}))

        for row_id, updated in updates:
            self.accounts.replace_row_in_transaction(row_id, updated)

    def release_account_leases(self, job_id: str) -> None:
        for account in self.accounts.search(Query().leaseJobId == job_id):
            self.accounts.update(
                {"leaseJobId": None, "leaseExpiresAt": None, "updatedAt": _utc_now()},
                Query().executorAccountId == account["executorAccountId"],
            )


def request_from_job(job: dict[str, Any]) -> JobStartRequest:
    return JobStartRequest.model_validate(
        {
            "idempotencyKey": job["idempotencyKey"],
            "orderId": job["orderId"],
            "platform": job["platform"],
            "actionType": job["actionType"],
            "quantity": job["quantity"],
            "accountSelector": job.get("accountSelector") or {"mode": "system"},
            "payload": job.get("payload") or {},
            "policyEnvelope": job.get("policyEnvelope"),
            "callbackUrl": job.get("callbackUrl"),
        },
    )


class ExecutorWorker:
    """Single-process durable worker for jobs persisted by the module facade."""

    def __init__(self, store: SspanelExecutorStore, worker_id: Optional[str] = None):
        self.store = store
        self.worker_id = worker_id or f"{MODULE_ID}:{os.getpid()}:{uuid.uuid4().hex}"
        self.stop_event = asyncio.Event()
        self.task: Optional[asyncio.Task[None]] = None

    def start(self) -> asyncio.Task[None]:
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.run(), name=f"{MODULE_ID}-worker")
        return self.task

    def stop(self) -> None:
        self.stop_event.set()
        if self.task is not None and not self.task.done():
            self.task.cancel()

    async def run(self) -> None:
        recovered = self.store.recover_jobs(self.worker_id)
        if recovered:
            logger.info("Recovered %s Instagram executor jobs", recovered)
        while not self.stop_event.is_set():
            claimed = await self.run_once()
            delivered = await self.deliver_outbox_once()
            if claimed or delivered:
                continue
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=_worker_poll_interval_seconds())
            except asyncio.TimeoutError:
                continue

    async def run_once(self) -> bool:
        job = self.store.claim_next_job(self.worker_id)
        if not job:
            return False
        heartbeat = asyncio.create_task(self._heartbeat(job["jobId"]), name=f"{MODULE_ID}-lease-heartbeat")
        try:
            request = request_from_job(job)
            if request.actionType in WRITE_ACTIONS:
                self.store.update_job(
                    job["jobId"],
                    {
                        "providerCallStartedAt": _utc_now(),
                        "providerAttempt": int(job.get("providerAttempt", 0)) + 1,
                    },
                )
            await execute_supported_job(job, request, self.store)
            self.store.update_job(
                job["jobId"],
                {
                    "providerCallStartedAt": None,
                    "lastProviderCallAt": _utc_now(),
                },
            )
            current = self.store.find_job(job["jobId"])
            completed_delta = max(
                0,
                int((current or {}).get("completedCount", 0)) - int(job.get("completedCount", 0)),
            )
            assigned_account_ids = (current or {}).get("assignedAccountIds") or job.get("assignedAccountIds") or []
            current_result = (current or {}).get("result")
            action_metadata = {}
            target_ref = None
            if isinstance(current_result, dict):
                for key in (
                    "phase",
                    "mediaId",
                    "commentId",
                    "threadId",
                    "messageId",
                    "affectedCommentIds",
                    "publishedCandidateIds",
                    "failedCandidateIds",
                ):
                    if key in current_result:
                        action_metadata[key] = current_result[key]
                if isinstance(current_result.get("mediaId"), str):
                    target_ref = current_result["mediaId"]
                elif isinstance(current_result.get("threadId"), str):
                    target_ref = current_result["threadId"]
                elif isinstance(current_result.get("messageId"), str):
                    target_ref = current_result["messageId"]
            if completed_delta > 0:
                usage_metric = _usage_metric(request.actionType)
                self.store.append_event(
                    job["jobId"],
                    "action.completed",
                    action_type=request.actionType,
                    status=(current or {}).get("status"),
                    executor_account_id=assigned_account_ids[0] if assigned_account_ids else None,
                    target_ref=target_ref,
                    quantity=completed_delta,
                    completed_count=(current or {}).get("completedCount"),
                    total_count=(current or {}).get("totalCount"),
                    usage={"metric": usage_metric, "amount": completed_delta, "unit": "actions"}
                    if usage_metric else None,
                    metadata=action_metadata,
                )
            elif current and current.get("status") in {"failed", "challenge_required", "session_expired", "banned_or_locked"}:
                self.store.append_event(
                    job["jobId"],
                    "action.failed",
                    action_type=request.actionType,
                    status=current.get("status"),
                    executor_account_id=assigned_account_ids[0] if assigned_account_ids else None,
                    error_code=current.get("errorCode"),
                    error_message=current.get("errorMessage"),
                    metadata=action_metadata,
                )
            if current and current.get("status") == "running":
                self.store.update_job(
                    job["jobId"],
                    {
                        "status": "failed",
                        "errorCode": "executor_no_terminal_transition",
                        "errorMessage": "Executor action did not produce a terminal or paused state",
                    },
                )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.exception("Instagram executor job failed: %s", _redact({"jobId": job["jobId"]}))
            self.store.update_job(
                job["jobId"],
                {
                    "status": "failed",
                    "errorCode": "executor_worker_error",
                    "errorMessage": _safe_error_message(error),
                    "providerCallStartedAt": None,
                },
            )
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
        return True

    async def deliver_outbox_once(self) -> bool:
        row = self.store.claim_outbox(self.worker_id)
        if not row:
            return False
        callback_url = row.get("callbackUrl")
        secret = _callback_secret()
        try:
            if not isinstance(callback_url, str) or not secret:
                raise RuntimeError("callback delivery is not configured")
            response = await asyncio.to_thread(
                requests.post,
                callback_url,
                json=row.get("payload") or {},
                headers={
                    "Content-Type": "application/json",
                    "X-External-Executor-Callback-Secret": secret,
                },
                timeout=max(1.0, min(float(os.getenv("SSPANEL_CALLBACK_TIMEOUT_SECONDS", "10")), 60.0)),
            )
            if response.status_code < 200 or response.status_code >= 300:
                raise RuntimeError(f"callback returned HTTP {response.status_code}")
            self.store.mark_outbox_delivered(row["outboxId"])
        except Exception as error:
            self.store.mark_outbox_failed(row, str(error))
            logger.warning(
                "SS-panel callback delivery failed",
                extra={"outboxId": row.get("outboxId"), "attempts": row.get("attempts", 0)},
            )
        return True

    async def _heartbeat(self, job_id: str) -> None:
        interval = max(1.0, _worker_lease_ttl_seconds() / 3)
        while True:
            await asyncio.sleep(interval)
            if not self.store.renew_job_lease(job_id, self.worker_id):
                return


def get_store() -> SspanelExecutorStore:
    global _store
    if _store is None:
        _store = SspanelExecutorStore(_store_path())
    return _store


def start_worker(store: Optional[SspanelExecutorStore] = None) -> ExecutorWorker:
    global _worker
    selected_store = store or get_store()
    if _worker is not None and _worker.store is selected_store and _worker.task is not None and not _worker.task.done():
        return _worker
    if _worker is not None:
        _worker.stop()
    _worker = ExecutorWorker(selected_store)
    _worker.start()
    return _worker


def stop_worker() -> None:
    global _worker
    if _worker is not None:
        _worker.stop()
        _worker = None


def reset_store_for_tests() -> None:
    global _store
    stop_worker()
    if _store is not None:
        _store.close()
        _store = None


def require_sspanel_api_key(
    x_sspanel_executor_key: Optional[str] = Security(sspanel_executor_key_header),
    x_sspanel_executor_key_id: Optional[str] = Security(sspanel_executor_key_id_header),
) -> dict[str, Any]:
    credential = _authenticate_executor_key(x_sspanel_executor_key, x_sspanel_executor_key_id)
    if not credential:
        raise HTTPException(status_code=401, detail="Invalid SS-panel executor API key")
    return credential


def require_sspanel_scope(scope: str):
    def dependency(
        x_sspanel_executor_key: Optional[str] = Security(sspanel_executor_key_header),
        x_sspanel_executor_key_id: Optional[str] = Security(sspanel_executor_key_id_header),
    ) -> dict[str, Any]:
        credential = _authenticate_executor_key(x_sspanel_executor_key, x_sspanel_executor_key_id)
        if not credential:
            raise HTTPException(status_code=401, detail="Invalid SS-panel executor API key")
        if "*" not in credential["scopes"] and scope not in credential["scopes"]:
            raise HTTPException(status_code=403, detail=f"Executor credential lacks scope: {scope}")
        return credential

    return dependency


@router.get(
    "/manifest",
    response_model=ModuleManifestResponse,
    dependencies=[Depends(require_sspanel_scope("manifest:read"))],
)
async def get_module_manifest() -> ModuleManifestResponse:
    supports_callbacks = bool(os.getenv("SSPANEL_CALLBACK_SECRET") and os.getenv("SSPANEL_CALLBACK_URL"))
    features = [
        feature for feature in CONTRACT_FEATURES
        if feature != "callbacks.v1" or supports_callbacks
    ]
    return ModuleManifestResponse(
        moduleId=MODULE_ID,
        platform="instagram",
        contractVersions=list(CONTRACT_VERSIONS),
        features=features,
        capabilities=sorted(IMPLEMENTED_CAPABILITIES),
        inputSchemas={
            capability: CAPABILITY_INPUT_SCHEMAS[capability]
            for capability in sorted(IMPLEMENTED_CAPABILITIES)
            if capability in CAPABILITY_INPUT_SCHEMAS
        },
        workflowTypes=list(IMPLEMENTED_WORKFLOW_TYPES),
        supportsPolling=True,
        supportsCallbacks=supports_callbacks,
    )


@router.get("/health", response_model=ModuleHealthResponse)
async def get_module_health() -> ModuleHealthResponse:
    return ModuleHealthResponse(moduleId=MODULE_ID, status="ok")


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
                "errorMessage": _safe_error_message(error),
                "result": {},
                "accountHealth": {
                    "executorAccountId": account["executorAccountId"],
                    "status": external_status,
                },
            },
        )


async def execute_profile_get_job(
    job: dict[str, Any],
    request: JobStartRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    if request.actionType != "instagram.profile.get":
        return job
    account = store.resolve_specific_account(request)
    if not account:
        return job

    try:
        client = create_instagram_profile_client(account)
        if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
            await client.login_by_sessionid(account["sessionid"])

        payload = request.payload
        username = payload.get("username")
        user_id = payload.get("userId")
        if username is not None and (not isinstance(username, str) or not username.strip()):
            raise ValueError("payload.username must be a non-empty string")
        if user_id is not None and (not isinstance(user_id, (str, int)) or not str(user_id).strip()):
            raise ValueError("payload.userId must be a non-empty string or integer")
        if username is not None and user_id is not None:
            raise ValueError("payload.username and payload.userId are mutually exclusive")

        if username is not None:
            raw_profile = await client.user_info_by_username(username.strip().lstrip("@"))
        elif user_id is not None:
            raw_profile = await client.user_info(str(user_id).strip())
        else:
            raw_profile = await client.account_info()

        profile = _redact(_jsonable(raw_profile))
        return store.update_job(
            job["jobId"],
            {
                "status": "completed",
                "completedCount": 1,
                "totalCount": job.get("totalCount", request.quantity),
                "result": {
                    "profile": profile if isinstance(profile, dict) else {"value": profile},
                    "executorAccountId": account["executorAccountId"],
                },
                "accountHealth": {
                    "executorAccountId": account["executorAccountId"],
                    "status": "healthy",
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
                "errorMessage": _safe_error_message(error),
                "result": {},
                "accountHealth": {
                    "executorAccountId": account["executorAccountId"],
                    "status": external_status,
                },
            },
        )


async def execute_comments_list_job(
    job: dict[str, Any],
    request: JobStartRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    if request.actionType != "instagram.comments.list":
        return job
    account = store.resolve_specific_account(request)
    if not account:
        return job

    try:
        client = create_instagram_comments_client(account)
        if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
            await client.login_by_sessionid(account["sessionid"])

        media_id = request.payload.get("mediaId")
        if not isinstance(media_id, str) or not media_id.strip():
            raise ValueError("payload.mediaId must be a non-empty string")
        amount = request.payload.get("amount", 20)
        if not isinstance(amount, int) or isinstance(amount, bool) or amount < 1 or amount > 100:
            raise ValueError("payload.amount must be an integer between 1 and 100")
        cursor = request.payload.get("cursor")
        if cursor is not None and not isinstance(cursor, str):
            raise ValueError("payload.cursor must be a string")

        raw_page = await client.media_comments_chunk(media_id.strip(), amount, cursor or None)
        if isinstance(raw_page, tuple) and len(raw_page) == 2:
            raw_comments, next_cursor = raw_page
        elif isinstance(raw_page, dict):
            raw_comments = raw_page.get("items", raw_page.get("comments", []))
            next_cursor = raw_page.get("nextCursor") or raw_page.get("next_cursor")
        else:
            raw_comments, next_cursor = raw_page, None
        comments = _jsonable(raw_comments)
        normalized_comments = comments if isinstance(comments, list) else []
        result = {
            "mediaId": media_id.strip(),
            "comments": [item for item in normalized_comments if isinstance(item, dict)],
            "executorAccountId": account["executorAccountId"],
        }
        if next_cursor:
            result["nextCursor"] = str(next_cursor)
        return store.update_job(
            job["jobId"],
            {
                "status": "completed",
                "completedCount": 1,
                "totalCount": job.get("totalCount", request.quantity),
                "result": _redact(result),
                "accountHealth": {
                    "executorAccountId": account["executorAccountId"],
                    "status": "healthy",
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
                "errorMessage": _safe_error_message(error),
                "result": {},
                "accountHealth": {
                    "executorAccountId": account["executorAccountId"],
                    "status": external_status,
                },
            },
        )


async def execute_comments_reply_job(
    job: dict[str, Any],
    request: JobStartRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    if request.actionType != "instagram.comments.reply":
        return job
    account = store.resolve_specific_account(request)
    if not account:
        return job

    try:
        client = create_instagram_comments_client(account)
        if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
            await client.login_by_sessionid(account["sessionid"])

        media_id = request.payload.get("mediaId")
        text = request.payload.get("text")
        if not isinstance(media_id, str) or not media_id.strip():
            raise ValueError("payload.mediaId must be a non-empty string")
        if not isinstance(text, str) or not text.strip():
            raise ValueError("payload.text must be a non-empty string")
        if len(text) > 2200:
            raise ValueError("payload.text exceeds Instagram comment length limit")
        replied_to = request.payload.get("repliedToCommentId")
        if replied_to is not None:
            if isinstance(replied_to, bool) or not str(replied_to).isdigit():
                raise ValueError("payload.repliedToCommentId must be numeric")
            replied_to = int(replied_to)

        comment = await client.media_comment(media_id.strip(), text.strip(), replied_to)
        normalized_comment = _redact(_jsonable(comment))
        return store.update_job(
            job["jobId"],
            {
                "status": "completed",
                "completedCount": 1,
                "totalCount": job.get("totalCount", request.quantity),
                "result": {
                    "mediaId": media_id.strip(),
                    "comment": normalized_comment if isinstance(normalized_comment, dict) else {"value": normalized_comment},
                    "executorAccountId": account["executorAccountId"],
                },
                "accountHealth": {
                    "executorAccountId": account["executorAccountId"],
                    "status": "healthy",
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
                "errorMessage": _safe_error_message(error),
                "result": {},
                "accountHealth": {
                    "executorAccountId": account["executorAccountId"],
                    "status": external_status,
                },
            },
        )


async def execute_media_upload_job(
    job: dict[str, Any],
    request: JobStartRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    if request.actionType not in {
        "instagram.media.upload.photo",
        "instagram.media.upload.video",
        "instagram.media.upload.reel",
        "instagram.story.upload",
    }:
        return job
    account = store.resolve_specific_account(request)
    if not account:
        return job

    media_path: Optional[Path] = None
    thumbnail_path: Optional[Path] = None
    try:
        payload_model: MediaUploadPayload | StoryUploadPayload
        if request.actionType == "instagram.story.upload":
            payload_model = StoryUploadPayload.model_validate(request.payload)
        else:
            payload_model = MediaUploadPayload.model_validate(request.payload)
        media_path = _download_media_to_temp(payload_model.mediaUrl, ".mp4" if request.actionType in {
            "instagram.media.upload.video",
            "instagram.media.upload.reel",
        } or getattr(payload_model, "mediaType", None) == "video" else ".jpg")
        if payload_model.thumbnailUrl:
            thumbnail_path = _download_media_to_temp(payload_model.thumbnailUrl, ".jpg")

        client = create_instagram_media_client(account)
        if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
            await client.login_by_sessionid(account["sessionid"])
        caption = payload_model.caption
        if request.actionType == "instagram.media.upload.photo":
            media = await client.photo_upload(media_path, caption)
            result_key = "media"
        elif request.actionType == "instagram.media.upload.video":
            media = await client.video_upload(media_path, caption, thumbnail=thumbnail_path)
            result_key = "media"
        elif request.actionType == "instagram.media.upload.reel":
            media = await client.clip_upload(media_path, caption, thumbnail=thumbnail_path)
            result_key = "media"
        elif payload_model.mediaType == "photo":
            media = await client.photo_upload_to_story(media_path, caption)
            result_key = "story"
        else:
            media = await client.video_upload_to_story(media_path, caption, thumbnail=thumbnail_path)
            result_key = "story"

        normalized = _redact(_jsonable(media))
        media_id = _result_identifier(normalized, "pk", "id", "media_id")
        return store.update_job(
            job["jobId"],
            {
                "status": "completed",
                "completedCount": 1,
                "totalCount": 1,
                "result": {
                    result_key: normalized if isinstance(normalized, dict) else {"value": normalized},
                    "executorAccountId": account["executorAccountId"],
                    **({"mediaId": media_id} if media_id else {}),
                },
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": "healthy"},
            },
        )
    except Exception as error:
        external_status = _exception_status(error)
        return store.update_job(
            job["jobId"],
            {
                "status": external_status,
                "completedCount": 0,
                "totalCount": 1,
                "errorCode": external_status,
                "errorMessage": _safe_error_message(error),
                "result": {},
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": external_status},
            },
        )
    finally:
        _remove_temp_path(media_path)
        _remove_temp_path(thumbnail_path)


async def execute_comments_moderation_job(
    job: dict[str, Any],
    request: JobStartRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    if request.actionType not in {"instagram.comments.delete", "instagram.comments.pin"}:
        return job
    account = store.resolve_specific_account(request)
    if not account:
        return job

    try:
        payload = CommentModerationPayload.model_validate(request.payload)
        comment_ids = [str(comment_id) for comment_id in payload.commentIds]
        client = create_instagram_comments_client(account)
        if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
            await client.login_by_sessionid(account["sessionid"])
        if request.actionType == "instagram.comments.delete":
            success = await client.comment_bulk_delete(payload.mediaId, [int(comment_id) for comment_id in comment_ids])
        else:
            success = True
            for comment_id in comment_ids:
                success = bool(await client.comment_pin(payload.mediaId, int(comment_id))) and success
        if not success:
            raise RuntimeError("Instagram comment moderation operation was not accepted")
        return store.update_job(
            job["jobId"],
            {
                "status": "completed",
                "completedCount": len(comment_ids),
                "totalCount": len(comment_ids),
                "result": {
                    "mediaId": payload.mediaId,
                    "affectedCommentIds": comment_ids,
                    "executorAccountId": account["executorAccountId"],
                },
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": "healthy"},
            },
        )
    except Exception as error:
        external_status = _exception_status(error)
        return store.update_job(
            job["jobId"],
            {
                "status": external_status,
                "completedCount": 0,
                "totalCount": len(request.payload.get("commentIds", [])) or 1,
                "errorCode": external_status,
                "errorMessage": _safe_error_message(error),
                "result": {},
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": external_status},
            },
        )


async def execute_dm_inbox_job(
    job: dict[str, Any],
    request: JobStartRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    if request.actionType != "instagram.dm.inbox":
        return job
    account = store.resolve_specific_account(request)
    if not account:
        return job

    try:
        payload = DmInboxPayload.model_validate(request.payload)
        client = create_instagram_direct_client(account)
        if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
            await client.login_by_sessionid(account["sessionid"])
        threads = await client.direct_threads(
            amount=payload.amount,
            selected_filter=payload.selectedFilter or "",
            box=payload.box or "",
            thread_message_limit=payload.threadMessageLimit,
        )
        normalized = _redact(_jsonable(threads))
        return store.update_job(
            job["jobId"],
            {
                "status": "completed",
                "completedCount": 1,
                "totalCount": 1,
                "result": {
                    "threads": normalized if isinstance(normalized, list) else [],
                    "executorAccountId": account["executorAccountId"],
                },
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": "healthy"},
            },
        )
    except Exception as error:
        external_status = _exception_status(error)
        return store.update_job(
            job["jobId"],
            {
                "status": external_status,
                "completedCount": 0,
                "totalCount": 1,
                "errorCode": external_status,
                "errorMessage": _safe_error_message(error),
                "result": {},
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": external_status},
            },
        )


async def execute_dm_send_job(
    job: dict[str, Any],
    request: JobStartRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    if request.actionType not in {"instagram.dm.send", "instagram.dm.reply"}:
        return job
    account = store.resolve_specific_account(request)
    if not account:
        return job

    try:
        client = create_instagram_direct_client(account)
        if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
            await client.login_by_sessionid(account["sessionid"])
        if request.actionType == "instagram.dm.send":
            payload = DmSendPayload.model_validate(request.payload)
            message = await client.direct_send(
                payload.text,
                user_ids=[int(user_id) for user_id in payload.userIds or []],
                thread_ids=[int(thread_id) for thread_id in payload.threadIds or []],
            )
        else:
            payload = DmReplyPayload.model_validate(request.payload)
            message = await client.direct_answer(int(payload.threadId), payload.text)
        normalized = _redact(_jsonable(message))
        message_id = _result_identifier(normalized, "pk", "id", "message_id", "messageId")
        thread_id = _result_identifier(normalized, "thread_id", "threadId")
        return store.update_job(
            job["jobId"],
            {
                "status": "completed",
                "completedCount": 1,
                "totalCount": 1,
                "result": {
                    "message": normalized if isinstance(normalized, dict) else {"value": normalized},
                    "executorAccountId": account["executorAccountId"],
                    **({"messageId": message_id} if message_id else {}),
                    **({"threadId": thread_id} if thread_id else {}),
                },
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": "healthy"},
            },
        )
    except Exception as error:
        external_status = _exception_status(error)
        return store.update_job(
            job["jobId"],
            {
                "status": external_status,
                "completedCount": 0,
                "totalCount": 1,
                "errorCode": external_status,
                "errorMessage": _safe_error_message(error),
                "result": {},
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": external_status},
            },
        )


async def execute_insights_job(
    job: dict[str, Any],
    request: JobStartRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    if request.actionType != "instagram.insights.basic":
        return job
    account = store.resolve_specific_account(request)
    if not account:
        return job

    try:
        payload = InsightsPayload.model_validate(request.payload)
        client = create_instagram_insights_client(account)
        if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
            await client.login_by_sessionid(account["sessionid"])
        if payload.mediaId:
            insights = await client.insights_media(payload.mediaId)
            scope = "media"
        else:
            insights = await client.insights_account()
            scope = "account"
        normalized = _redact(_jsonable(insights))
        return store.update_job(
            job["jobId"],
            {
                "status": "completed",
                "completedCount": 1,
                "totalCount": 1,
                "result": {
                    "scope": scope,
                    "insights": normalized if isinstance(normalized, dict) else {"value": normalized},
                    "executorAccountId": account["executorAccountId"],
                    **({"mediaId": payload.mediaId} if payload.mediaId else {}),
                },
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": "healthy"},
            },
        )
    except Exception as error:
        external_status = _exception_status(error)
        return store.update_job(
            job["jobId"],
            {
                "status": external_status,
                "completedCount": 0,
                "totalCount": 1,
                "errorCode": external_status,
                "errorMessage": _safe_error_message(error),
                "result": {},
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": external_status},
            },
        )


async def execute_instagram_warmup_job(
    job: dict[str, Any],
    request: JobStartRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    if request.actionType != "instagram.warmup":
        return job
    account = store.resolve_specific_account(request)
    if not account:
        return job

    try:
        payload = request.payload
        action_mix = payload.get("actionMix", ["instagram.account.health"])
        if (
            not isinstance(action_mix, list)
            or not action_mix
            or any(action not in {"instagram.account.health", "instagram.profile.get", "instagram.comments.list"} for action in action_mix)
        ):
            raise ValueError("payload.actionMix must contain supported Instagram warmup actions")
        targets = payload.get("targetIds", payload.get("mediaIds", []))
        if not isinstance(targets, list) or any(not isinstance(target, str) or not target.strip() for target in targets):
            raise ValueError("payload.targetIds must be a list of non-empty strings")

        state = job.get("result") if isinstance(job.get("result"), dict) else {}
        cursor = int(state.get("cursor", job.get("completedCount", 0)))
        started_at = state.get("startedAt") or job.get("acceptedAt") or _utc_now()
        duration_minutes = request.policyEnvelope.durationMinutes if request.policyEnvelope else payload.get("durationMinutes")
        if duration_minutes is not None:
            if not isinstance(duration_minutes, (int, float)) or isinstance(duration_minutes, bool) or duration_minutes <= 0:
                raise ValueError("warmup durationMinutes must be a positive number")
            try:
                started_at_value = datetime.fromisoformat(str(started_at).replace("Z", "+00:00"))
            except ValueError:
                started_at_value = datetime.now(timezone.utc)
            if cursor > 0 and datetime.now(timezone.utc) >= started_at_value + timedelta(minutes=float(duration_minutes)):
                return store.update_job(
                    job["jobId"],
                    {
                        "status": "completed",
                        "completedCount": cursor,
                        "totalCount": request.quantity,
                        "nextRunAt": None,
                        "result": {
                            "phase": "warmup",
                            "cursor": cursor,
                            "startedAt": started_at,
                            "stoppedReason": "duration_elapsed",
                            "accountId": account["executorAccountId"],
                        },
                    },
                )
        action = str(action_mix[cursor % len(action_mix)])
        target = str(targets[cursor % len(targets)]).strip() if targets else None
        if action == "instagram.account.health":
            client = create_instagram_health_client(account)
            if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
                await client.login_by_sessionid(account["sessionid"])
            await client.account_info()
        elif action == "instagram.profile.get":
            client = create_instagram_profile_client(account)
            if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
                await client.login_by_sessionid(account["sessionid"])
            if target:
                await client.user_info_by_username(target.lstrip("@"))
            else:
                await client.account_info()
        else:
            if not target:
                raise ValueError("instagram.comments.list warmup action requires payload.targetIds")
            client = create_instagram_comments_client(account)
            if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
                await client.login_by_sessionid(account["sessionid"])
            await client.media_comments_chunk(target, 1, None)

        completed = cursor + 1
        risk_profile = request.policyEnvelope.riskProfile if request.policyEnvelope else "standard"
        configured_delay = float(os.getenv("SSPANEL_EXECUTOR_WARMUP_PACING_SECONDS", "1"))
        default_delay = {"safe": 5.0, "standard": 1.0, "fast": 0.1}.get(risk_profile, 1.0)
        delay = max(0.01, min(configured_delay if configured_delay >= 0 else default_delay, 3600.0))
        next_run_at = _utc_after_seconds(int(delay))
        result = {
            "phase": "warmup",
            "cursor": completed,
            "startedAt": started_at,
            "lastAction": action,
            "lastTarget": target,
            "accountId": account["executorAccountId"],
        }
        if completed >= request.quantity:
            return store.update_job(
                job["jobId"],
                {
                    "status": "completed",
                    "completedCount": completed,
                    "totalCount": request.quantity,
                    "nextRunAt": None,
                    "result": result,
                    "accountHealth": {"executorAccountId": account["executorAccountId"], "status": "healthy"},
                },
            )
        return store.update_job(
            job["jobId"],
            {
                "status": "queued",
                "completedCount": completed,
                "totalCount": request.quantity,
                "nextRunAt": next_run_at,
                "result": result,
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": "healthy"},
            },
        )
    except Exception as error:
        external_status = _exception_status(error)
        return store.update_job(
            job["jobId"],
            {
                "status": external_status,
                "errorCode": external_status,
                "errorMessage": _safe_error_message(error),
                "result": {"phase": "warmup", "cursor": job.get("completedCount", 0)},
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": external_status},
            },
        )


async def execute_smart_comments_discovery_job(
    job: dict[str, Any],
    request: JobStartRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    if request.actionType != "instagram.comments.smart_reply":
        return job
    account = store.resolve_specific_account(request)
    if not account:
        return job

    try:
        client = create_instagram_comments_client(account)
        if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
            await client.login_by_sessionid(account["sessionid"])

        media_ids = request.payload.get("mediaIds")
        if (
            not isinstance(media_ids, list)
            or not media_ids
            or len(media_ids) > 20
            or any(not isinstance(media_id, str) or not media_id.strip() for media_id in media_ids)
        ):
            raise ValueError("payload.mediaIds must contain between 1 and 20 non-empty strings")
        max_candidates = request.payload.get("maxCandidates", request.quantity)
        if not isinstance(max_candidates, int) or isinstance(max_candidates, bool) or max_candidates < 1 or max_candidates > 100:
            raise ValueError("payload.maxCandidates must be an integer between 1 and 100")
        scenario_ref = request.payload.get("scenarioRef")
        if scenario_ref is not None and (not isinstance(scenario_ref, str) or not scenario_ref.strip()):
            raise ValueError("payload.scenarioRef must be a non-empty string")

        candidates: list[dict[str, Any]] = []
        for media_id in media_ids:
            raw_page = await client.media_comments_chunk(media_id.strip(), min(max_candidates, 100), None)
            if isinstance(raw_page, tuple) and len(raw_page) == 2:
                raw_comments = raw_page[0]
            elif isinstance(raw_page, dict):
                raw_comments = raw_page.get("items", raw_page.get("comments", []))
            else:
                raw_comments = raw_page
            normalized_comments = _jsonable(raw_comments)
            if not isinstance(normalized_comments, list):
                continue
            for comment in normalized_comments:
                if not isinstance(comment, dict):
                    continue
                comment_id = comment.get("pk") or comment.get("id")
                comment_text = comment.get("text")
                if comment_id is None or not isinstance(comment_text, str) or not comment_text.strip():
                    continue
                user = comment.get("user") if isinstance(comment.get("user"), dict) else {}
                candidates.append({
                    "candidateId": f"{media_id.strip()}:{comment_id}",
                    "mediaId": media_id.strip(),
                    "commentId": str(comment_id),
                    "text": comment_text.strip(),
                    **({"authorUsername": user.get("username")} if isinstance(user.get("username"), str) else {}),
                })
                if len(candidates) >= max_candidates:
                    break
            if len(candidates) >= max_candidates:
                break

        if not candidates:
            return store.update_job(
                job["jobId"],
                {
                    "status": "completed",
                    "completedCount": 0,
                    "totalCount": job.get("totalCount", request.quantity),
                    "result": {"phase": "awaiting_content", "candidates": [], **({"scenarioRef": scenario_ref} if scenario_ref else {})},
                    "accountHealth": {"executorAccountId": account["executorAccountId"], "status": "healthy"},
                },
            )
        return store.update_job(
            job["jobId"],
            {
                "status": "paused",
                "result": {
                    "phase": "awaiting_content",
                    "candidates": candidates,
                    **({"scenarioRef": scenario_ref} if scenario_ref else {}),
                },
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": "healthy"},
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
                "errorMessage": _safe_error_message(error),
                "result": {},
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": external_status},
            },
        )


async def execute_smart_comments_input_job(
    job: dict[str, Any],
    request: WorkflowInputRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    result = job.get("result") or {}
    candidate_map = {
        candidate.get("candidateId"): candidate
        for candidate in result.get("candidates", [])
        if isinstance(candidate, dict) and candidate.get("candidateId")
    }
    account = store.resolve_specific_account(
        JobStartRequest(
            idempotencyKey=job["idempotencyKey"],
            orderId=job["orderId"],
            platform="instagram",
            actionType="instagram.comments.smart_reply",
            quantity=job.get("quantity", 1),
            accountSelector=job.get("accountSelector") or {"mode": "system"},
            payload=job.get("payload") or {},
        ),
    )
    if not account:
        raise HTTPException(status_code=422, detail="Workflow input requires a specific leased account")

    try:
        client = create_instagram_comments_client(account)
        if account.get("sessionid") and not account.get("settings") and hasattr(client, "login_by_sessionid"):
            await client.login_by_sessionid(account["sessionid"])
        published_ids = set(job.get("publishedCandidateIds") or [])
        failed_ids: list[str] = []
        for decision in request.decisions:
            if decision.decision != "publish" or decision.candidateId in published_ids:
                continue
            candidate = candidate_map.get(decision.candidateId)
            if not candidate or not decision.text.strip():
                failed_ids.append(decision.candidateId)
                continue
            await client.media_comment(
                str(candidate["mediaId"]),
                decision.text.strip(),
                int(str(candidate["commentId"])),
            )
            published_ids.add(decision.candidateId)

        target = int(job.get("totalCount", len(request.decisions)))
        status_value: JobStatus = "completed" if len(published_ids) >= target else "partially_completed"
        return store.update_job(
            job["jobId"],
            {
                "status": status_value,
                "completedCount": len(published_ids),
                "totalCount": target,
                "workflowInputIdempotencyKey": request.inputIdempotencyKey,
                "publishedCandidateIds": sorted(published_ids),
                "result": {
                    "phase": "publishing",
                    "publishedCandidateIds": sorted(published_ids),
                    "failedCandidateIds": failed_ids,
                },
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": "healthy"},
            },
        )
    except Exception as error:
        external_status = _exception_status(error)
        return store.update_job(
            job["jobId"],
            {
                "status": external_status,
                "errorCode": external_status,
                "errorMessage": _safe_error_message(error),
                "workflowInputIdempotencyKey": request.inputIdempotencyKey,
                "result": {"phase": "publishing", "publishedCandidateIds": sorted(job.get("publishedCandidateIds") or [])},
                "accountHealth": {"executorAccountId": account["executorAccountId"], "status": external_status},
            },
        )


async def execute_supported_job(
    job: dict[str, Any],
    request: JobStartRequest,
    store: SspanelExecutorStore,
) -> dict[str, Any]:
    if request.actionType == "instagram.account.health":
        return await execute_account_health_job(job, request, store)
    if request.actionType == "instagram.profile.get":
        return await execute_profile_get_job(job, request, store)
    if request.actionType in {
        "instagram.media.upload.photo",
        "instagram.media.upload.video",
        "instagram.media.upload.reel",
        "instagram.story.upload",
    }:
        return await execute_media_upload_job(job, request, store)
    if request.actionType == "instagram.comments.list":
        return await execute_comments_list_job(job, request, store)
    if request.actionType == "instagram.warmup":
        return await execute_instagram_warmup_job(job, request, store)
    if request.actionType == "instagram.comments.reply":
        return await execute_comments_reply_job(job, request, store)
    if request.actionType in {"instagram.comments.delete", "instagram.comments.pin"}:
        return await execute_comments_moderation_job(job, request, store)
    if request.actionType == "instagram.dm.inbox":
        return await execute_dm_inbox_job(job, request, store)
    if request.actionType in {"instagram.dm.send", "instagram.dm.reply"}:
        return await execute_dm_send_job(job, request, store)
    if request.actionType == "instagram.insights.basic":
        return await execute_insights_job(job, request, store)
    if request.actionType == "instagram.comments.smart_reply":
        if job.get("workflowInput"):
            input_request = WorkflowInputRequest.model_validate(job["workflowInput"])
            return await execute_smart_comments_input_job(job, input_request, store)
        return await execute_smart_comments_discovery_job(job, request, store)
    return job


@router.post(
    "/jobs",
    response_model=JobStartResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_sspanel_scope("jobs:write"))],
)
async def start_job(request: JobStartRequest, store: SspanelExecutorStore = Depends(get_store)) -> JobStartResponse:
    if request.actionType not in IMPLEMENTED_CAPABILITIES:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "UNSUPPORTED_CAPABILITY",
                "capability": request.actionType,
            },
        )
    job, _ = store.get_or_create_job(request)
    start_worker(store)
    logger.info(
        "Accepted SS-panel Instagram job: %s",
        _redact({"jobId": job["jobId"], "orderId": job["orderId"], "actionType": job["actionType"]}),
    )
    return JobStartResponse(jobId=job["jobId"], status=job["status"], acceptedAt=job["acceptedAt"])


@router.get(
    "/jobs/{job_id}",
    response_model=JobProgressResponse,
    response_model_exclude_none=True,
    dependencies=[Depends(require_sspanel_scope("jobs:read"))],
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
        nextRunAt=job.get("nextRunAt"),
        assignedAccountIds=job.get("assignedAccountIds"),
        errorCode=job.get("errorCode"),
        errorMessage=job.get("errorMessage"),
        result=job.get("result") or {},
        accountHealth=job.get("accountHealth") or {},
        eventSequence=job.get("eventSequence"),
        events=job.get("events") or None,
    )


@router.post(
    "/jobs/{job_id}/input",
    response_model=JobProgressResponse,
    response_model_exclude_none=True,
    dependencies=[Depends(require_sspanel_scope("jobs:write"))],
)
async def provide_workflow_input(
    job_id: str,
    request: WorkflowInputRequest,
    store: SspanelExecutorStore = Depends(get_store),
) -> JobProgressResponse:
    job = store.find_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="SS-panel executor job not found")
    updated = store.accept_workflow_input(job_id, request)
    start_worker(store)
    return JobProgressResponse(
        jobId=updated["jobId"],
        status=updated["status"],
        completedCount=updated.get("completedCount", 0),
        totalCount=updated.get("totalCount", 0),
        nextRunAt=updated.get("nextRunAt"),
        assignedAccountIds=updated.get("assignedAccountIds"),
        errorCode=updated.get("errorCode"),
        errorMessage=updated.get("errorMessage"),
        result=updated.get("result") or {},
        accountHealth=updated.get("accountHealth") or {},
        eventSequence=updated.get("eventSequence"),
        events=updated.get("events") or None,
    )


@router.post(
    "/jobs/{job_id}/pause",
    response_model=JobProgressResponse,
    response_model_exclude_none=True,
    dependencies=[Depends(require_sspanel_scope("jobs:write"))],
)
async def pause_job(job_id: str, store: SspanelExecutorStore = Depends(get_store)) -> JobProgressResponse:
    job = store.pause_job(job_id)
    return JobProgressResponse(
        jobId=job["jobId"],
        status=job["status"],
        completedCount=job.get("completedCount", 0),
        totalCount=job.get("totalCount", 0),
        nextRunAt=job.get("nextRunAt"),
        assignedAccountIds=job.get("assignedAccountIds"),
        errorCode=job.get("errorCode"),
        errorMessage=job.get("errorMessage"),
        result=job.get("result") or {},
        accountHealth=job.get("accountHealth") or {},
        eventSequence=job.get("eventSequence"),
        events=job.get("events") or None,
    )


@router.post(
    "/jobs/{job_id}/resume",
    response_model=JobProgressResponse,
    response_model_exclude_none=True,
    dependencies=[Depends(require_sspanel_scope("jobs:write"))],
)
async def resume_job(
    job_id: str,
    confirmProviderRetry: bool = False,
    store: SspanelExecutorStore = Depends(get_store),
) -> JobProgressResponse:
    job = store.resume_job(job_id, confirm_provider_retry=confirmProviderRetry)
    start_worker(store)
    return JobProgressResponse(
        jobId=job["jobId"],
        status=job["status"],
        completedCount=job.get("completedCount", 0),
        totalCount=job.get("totalCount", 0),
        nextRunAt=job.get("nextRunAt"),
        assignedAccountIds=job.get("assignedAccountIds"),
        errorCode=job.get("errorCode"),
        errorMessage=job.get("errorMessage"),
        result=job.get("result") or {},
        accountHealth=job.get("accountHealth") or {},
        eventSequence=job.get("eventSequence"),
        events=job.get("events") or None,
    )


@router.post(
    "/jobs/{job_id}/cancel",
    response_model=JobProgressResponse,
    response_model_exclude_none=True,
    dependencies=[Depends(require_sspanel_scope("jobs:write"))],
)
async def cancel_job(job_id: str, store: SspanelExecutorStore = Depends(get_store)) -> JobProgressResponse:
    job = store.cancel_job(job_id)
    return JobProgressResponse(
        jobId=job["jobId"],
        status=job["status"],
        completedCount=job.get("completedCount", 0),
        totalCount=job.get("totalCount", 0),
        nextRunAt=job.get("nextRunAt"),
        assignedAccountIds=job.get("assignedAccountIds"),
        result=job.get("result") or {},
        accountHealth=job.get("accountHealth") or {},
        eventSequence=job.get("eventSequence"),
        events=job.get("events") or None,
    )


@router.post(
    "/accounts/import-session",
    response_model=ImportSessionResponse,
    dependencies=[Depends(require_sspanel_scope("accounts:write"))],
)
async def import_session(
    request: ImportSessionRequest,
    store: SspanelExecutorStore = Depends(get_store),
) -> ImportSessionResponse:
    account = store.import_account(request)
    return ImportSessionResponse(executorAccountId=account["executorAccountId"], status=account["status"])


@router.get(
    "/accounts/{executor_account_id}/health",
    dependencies=[Depends(require_sspanel_scope("accounts:read"))],
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
        "username": account.get("username"),
        "lastSeenAt": account.get("updatedAt"),
        "leaseExpiresAt": account.get("leaseExpiresAt"),
        "lastCheckedAt": _utc_now(),
    }


@router.get(
    "/accounts",
    response_model=list[dict[str, Any]],
    dependencies=[Depends(require_sspanel_scope("accounts:read"))],
)
async def list_accounts(store: SspanelExecutorStore = Depends(get_store)) -> list[dict[str, Any]]:
    return [
        {
            "executorAccountId": account.get("executorAccountId"),
            "username": account.get("username"),
            "status": account.get("status", "unknown"),
            "updatedAt": account.get("updatedAt"),
            "leaseExpiresAt": account.get("leaseExpiresAt"),
        }
        for account in store.accounts.all()
    ]
