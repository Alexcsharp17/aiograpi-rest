from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LivePolicy:
    kind: str
    reason: str = ""
    verify_with: str = ""


SYSTEM_PATHS = {
    "/health",
    "/ready",
    "/metrics",
    "/build",
    "/deps",
    "/module/v1/health",
}

SESSION_MUTATIONS = {
    ("POST", "/auth/login"): "/account",
    ("POST", "/auth/login/by/sessionid"): "/account",
    ("PATCH", "/auth/relogin"): "/account",
    ("PATCH", "/auth/settings"): "/auth/settings",
}

REVERSIBLE_MUTATIONS = {
    ("PATCH", "/media"): "GET /media",
    ("POST", "/hashtag/follow"): "DELETE /hashtag/follow",
    ("DELETE", "/hashtag/follow"): "GET /hashtag",
    ("POST", "/media/like"): "DELETE /media/like",
    ("DELETE", "/media/like"): "GET /media",
    ("PATCH", "/media/seen"): "GET /media",
    ("POST", "/media/comment"): "GET /media/comments",
    ("DELETE", "/media/comment"): "GET /media/comments",
    ("POST", "/media/comment/check/offensive"): "same response",
    ("POST", "/media/comment/like"): "DELETE /media/comment/like",
    ("DELETE", "/media/comment/like"): "GET /media/comment/likers",
    ("POST", "/media/save"): "DELETE /media/save",
    ("DELETE", "/media/save"): "GET /account/liked/media",
    ("POST", "/media/note"): "GET /media",
    ("DELETE", "/media/note"): "GET /media",
    ("POST", "/note"): "GET /notes",
    ("DELETE", "/note"): "GET /notes",
    ("POST", "/note/music"): "GET /notes",
    ("PATCH", "/notes/last-seen"): "GET /notes",
    ("POST", "/story/like"): "DELETE /story/like",
    ("DELETE", "/story/like"): "GET /story",
    ("PATCH", "/story/seen"): "GET /story",
    ("POST", "/user/follow"): "GET /user/friendship",
    ("DELETE", "/user/follow"): "GET /user/friendship",
    ("DELETE", "/user/follower"): "GET /user/friendship",
    ("POST", "/user/close-friend"): "GET /user/friendship",
    ("DELETE", "/user/close-friend"): "GET /user/friendship",
    ("POST", "/user/mute/posts"): "GET /user/friendship",
    ("DELETE", "/user/mute/posts"): "GET /user/friendship",
    ("POST", "/user/mute/stories"): "GET /user/friendship",
    ("DELETE", "/user/mute/stories"): "GET /user/friendship",
    ("POST", "/user/notifications/posts"): "GET /user/friendship",
    ("DELETE", "/user/notifications/posts"): "GET /user/friendship",
    ("POST", "/user/notifications/stories"): "GET /user/friendship",
    ("DELETE", "/user/notifications/stories"): "GET /user/friendship",
    ("POST", "/user/notifications/reels"): "GET /user/friendship",
    ("DELETE", "/user/notifications/reels"): "GET /user/friendship",
    ("POST", "/user/notifications/videos"): "GET /user/friendship",
    ("DELETE", "/user/notifications/videos"): "GET /user/friendship",
    ("POST", "/user/block"): "GET /user/friendship",
    ("DELETE", "/user/block"): "GET /user/friendship",
}

CLEANUP_MUTATIONS = {
    ("DELETE", "/media"): "GET /media returns 404 or missing media",
    ("DELETE", "/story"): "GET /story returns 404 or missing story",
}

UPLOAD_MUTATIONS = {
    ("POST", "/album/upload"): "GET /media",
    ("POST", "/album/upload/with/music"): "GET /media",
    ("POST", "/clip/upload"): "GET /media",
    ("POST", "/clip/upload/by/url"): "GET /media",
    ("POST", "/clip/upload/with/music"): "GET /media",
    ("POST", "/igtv/upload"): "GET /media",
    ("POST", "/igtv/upload/by/url"): "GET /media",
    ("POST", "/photo/upload"): "GET /media",
    ("POST", "/photo/upload/by/url"): "GET /media",
    ("POST", "/photo/upload/with/music"): "GET /media",
    ("POST", "/story/upload"): "GET /story + GET /user/stories + GET /story/download",
    ("POST", "/story/upload/by/url"): "GET /story + GET /user/stories + GET /story/download",
    ("POST", "/video/upload"): "GET /media",
    ("POST", "/video/upload/by/url"): "GET /media",
}

SSPANEL_MUTATIONS = {
    ("POST", "/module/v1/accounts/import-session"): "GET /module/v1/accounts/{executor_account_id}/health",
    ("DELETE", "/module/v1/accounts/{executor_account_id}"): "GET /module/v1/accounts/{executor_account_id}/health returns 404",
    ("POST", "/module/v1/jobs"): "GET /module/v1/jobs/{job_id}",
    ("POST", "/module/v1/jobs/{job_id}/cancel"): "GET /module/v1/jobs/{job_id}",
    ("POST", "/module/v1/jobs/{job_id}/input"): "GET /module/v1/jobs/{job_id}",
    ("POST", "/module/v1/jobs/{job_id}/pause"): "GET /module/v1/jobs/{job_id}",
    ("POST", "/module/v1/jobs/{job_id}/resume"): "GET /module/v1/jobs/{job_id}",
}

GUARDED_PREFIX_REASONS = {
    ("/account", "account mutation"): "changes authenticated account state or depends on inbound follow requests",
    ("/auth/challenge", "challenge"): "requires a real active Instagram challenge",
    ("/auth/totp", "totp state"): "changes two-factor authentication state",
    ("/clip/pin", "owned reel"): "requires owned Reel fixture and profile cleanup",
    ("/direct", "direct fixture"): "requires controlled Direct threads, users, and messages",
    ("/highlight", "highlight fixture"): "requires owned story/highlight fixtures",
    ("/media/archive", "owned media"): "requires owned media and archive state cleanup",
    ("/media/livestream", "live broadcast"): "creates or mutates a real livestream",
    ("/media/pin", "owned media"): "requires owned media and visible profile cleanup",
    ("/media/comment/pin", "owned comment"): "requires owned media comments",
    ("/notifications", "account settings"): "changes notification settings on the authenticated account",
}

GUARDED_EXACT_REASONS: dict[tuple[str, str], str] = {
    ("POST", "/music/bookmark"): "changes saved music and aiograpi does not expose an unbookmark helper",
}


def operation_policy(method: str, path: str) -> LivePolicy:
    method = method.upper()

    if path in SYSTEM_PATHS:
        return LivePolicy("system")
    if (method, path) in SSPANEL_MUTATIONS:
        return LivePolicy("sspanel", verify_with=SSPANEL_MUTATIONS[(method, path)])
    if method == "GET" and path.endswith(("/download", "/download/by/url", "/download/by/urls")):
        return LivePolicy("download", verify_with="binary/media validation")
    if method == "GET":
        if path.startswith("/auth/"):
            return LivePolicy("session-read")
        return LivePolicy("read")

    if (method, path) in SESSION_MUTATIONS:
        return LivePolicy("session", verify_with=SESSION_MUTATIONS[(method, path)])
    if (method, path) in REVERSIBLE_MUTATIONS:
        return LivePolicy("reversible", verify_with=REVERSIBLE_MUTATIONS[(method, path)])
    if (method, path) in UPLOAD_MUTATIONS:
        return LivePolicy("upload", verify_with=UPLOAD_MUTATIONS[(method, path)])
    if (method, path) in CLEANUP_MUTATIONS:
        return LivePolicy("cleanup", verify_with=CLEANUP_MUTATIONS[(method, path)])

    guarded_reason = GUARDED_EXACT_REASONS.get((method, path))
    if guarded_reason:
        return LivePolicy("guarded", reason=guarded_reason)
    for prefix, reason in GUARDED_PREFIX_REASONS:
        if path.startswith(prefix):
            return LivePolicy("guarded", reason=reason)

    return LivePolicy("unclassified")


def guarded_operations() -> dict[tuple[str, str], LivePolicy]:
    from aiograpi_rest.main import app

    return {
        (method.upper(), path): policy
        for path, methods in app.openapi()["paths"].items()
        for method in methods
        if (policy := operation_policy(method, path)).kind == "guarded"
    }
