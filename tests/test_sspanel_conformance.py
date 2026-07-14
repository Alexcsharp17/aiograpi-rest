from httpx import ASGITransport, AsyncClient

from aiograpi_rest.main import app
from aiograpi_rest.routers import sspanel
from tests.conformance.sspanel_module_conformance import assert_sspanel_module_conformance


async def test_instagram_module_passes_reusable_sspanel_conformance(tmp_path, monkeypatch):
    monkeypatch.setenv("SSPANEL_EXECUTOR_API_KEY", "executor-secret")
    monkeypatch.setenv(
        "SSPANEL_EXECUTOR_ENCRYPTION_KEY",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
    )
    monkeypatch.setenv("SSPANEL_EXECUTOR_DB_PATH", str(tmp_path / "sspanel-conformance.sqlite3"))
    sspanel.reset_store_for_tests()
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            await assert_sspanel_module_conformance(client, "executor-secret")
    finally:
        sspanel.reset_store_for_tests()


async def test_legacy_executor_key_accepts_configured_key_id(tmp_path, monkeypatch):
    monkeypatch.setenv("SSPANEL_EXECUTOR_API_KEY", "executor-secret")
    monkeypatch.setenv(
        "SSPANEL_EXECUTOR_ENCRYPTION_KEY",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
    )
    monkeypatch.setenv("SSPANEL_EXECUTOR_API_KEY_ID", "active")
    monkeypatch.setenv("SSPANEL_EXECUTOR_DB_PATH", str(tmp_path / "sspanel-key-id.sqlite3"))
    sspanel.reset_store_for_tests()
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(
                "/module/v1/manifest",
                headers={
                    "X-SSPanel-Executor-Key": "executor-secret",
                    "X-SSPanel-Executor-Key-Id": "active",
                },
            )
        assert response.status_code == 200
    finally:
        sspanel.reset_store_for_tests()
