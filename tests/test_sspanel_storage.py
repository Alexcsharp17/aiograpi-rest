import json

import pytest

from aiograpi_rest.sspanel_storage import SQLiteJsonStore, StorageConfigurationError

KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


def test_storage_encrypts_rows_and_reopens_them(tmp_path, monkeypatch):
    monkeypatch.setenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", KEY)
    path = tmp_path / "executor.sqlite3"
    store = SQLiteJsonStore(str(path))
    jobs = store.table("jobs")
    jobs.insert({
        "jobId": "job-1",
        "idempotencyKey": "idem-1",
        "payload": {"sessionid": "raw-session-secret", "proxy": "http://proxy-pass@example.test"},
    })
    store.close()

    raw = path.read_bytes()
    assert b"raw-session-secret" not in raw
    assert b"proxy-pass" not in raw

    reopened = SQLiteJsonStore(str(path))
    try:
        assert reopened.table("jobs").get(lambda row: row.get("jobId") == "job-1")["payload"]["sessionid"] == "raw-session-secret"
    finally:
        reopened.close()


def test_storage_requires_an_explicit_key_outside_dev_mode(tmp_path, monkeypatch):
    monkeypatch.delenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("SSPANEL_EXECUTOR_ALLOW_INSECURE_DEV_STORAGE", raising=False)

    with pytest.raises(StorageConfigurationError, match="ENCRYPTION_KEY"):
        SQLiteJsonStore(str(tmp_path / "executor.sqlite3"))


def test_storage_reports_wrong_key_as_configuration_error(tmp_path, monkeypatch):
    path = tmp_path / "executor.sqlite3"
    monkeypatch.setenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", KEY)
    store = SQLiteJsonStore(str(path))
    store.table("jobs").insert({"jobId": "job-1", "idempotencyKey": "idem-1", "payload": {}})
    store.close()

    monkeypatch.setenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=")
    with pytest.raises(StorageConfigurationError, match="could not be decrypted"):
        SQLiteJsonStore(str(path))


def test_storage_migrates_legacy_tinydb_json_to_encrypted_sqlite(tmp_path, monkeypatch):
    monkeypatch.setenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", KEY)
    path = tmp_path / "executor.json"
    path.write_text(json.dumps({
        "jobs": {"1": {"jobId": "job-legacy", "idempotencyKey": "idem-legacy", "secret": "legacy-secret"}},
        "accounts": {},
        "usage": {},
    }))

    store = SQLiteJsonStore(str(path))
    try:
        assert store.table("jobs").get(lambda row: row.get("jobId") == "job-legacy")["secret"] == "legacy-secret"
    finally:
        store.close()

    assert (tmp_path / "executor.json.legacy.json").exists()
    assert b"legacy-secret" not in path.read_bytes()
