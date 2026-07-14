import base64
import json
import os
import sqlite3

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from aiograpi_rest.sspanel_storage import (
    SQLiteJsonStore,
    StorageConfigurationError,
    resolve_secret_provider,
)

KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
ROTATED_KEY = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="


class FakeSecretProvider:
    def __init__(self, values):
        self.values = values

    def get(self, name: str) -> str:
        return self.values.get(name, "")


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


def test_storage_reads_encryption_key_from_mounted_secret_file(tmp_path, monkeypatch):
    monkeypatch.delenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", raising=False)
    key_file = tmp_path / "encryption-key"
    key_file.write_text(KEY)
    monkeypatch.setenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY_FILE", str(key_file))

    store = SQLiteJsonStore(str(tmp_path / "executor.sqlite3"))
    try:
        store.table("jobs").insert({"jobId": "job-file-secret", "idempotencyKey": "idem-file-secret", "payload": {}})
        assert store.table("jobs").get(lambda row: row.get("jobId") == "job-file-secret") is not None
    finally:
        store.close()


def test_storage_accepts_a_deployment_secret_provider(tmp_path, monkeypatch):
    monkeypatch.delenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", raising=False)
    provider = FakeSecretProvider({"SSPANEL_EXECUTOR_ENCRYPTION_KEY": KEY})

    store = SQLiteJsonStore(str(tmp_path / "executor.sqlite3"), secret_provider=provider)
    try:
        store.table("jobs").insert({"jobId": "job-provider", "idempotencyKey": "idem-provider", "payload": {}})
        assert store.table("jobs").get(lambda row: row.get("jobId") == "job-provider") is not None
    finally:
        store.close()


def test_secret_provider_class_can_be_loaded_from_deployment_configuration(monkeypatch):
    monkeypatch.setenv(
        "SSPANEL_SECRET_PROVIDER_CLASS",
        "aiograpi_rest.sspanel_storage:EnvironmentSecretProvider",
    )
    provider = resolve_secret_provider()
    assert provider.get("MISSING_SECRET") == ""


def test_invalid_secret_provider_class_fails_closed(monkeypatch):
    monkeypatch.setenv("SSPANEL_SECRET_PROVIDER_CLASS", "missing.module:Provider")
    with pytest.raises(StorageConfigurationError, match="could not be loaded"):
        resolve_secret_provider()


def test_storage_requires_an_explicit_key_outside_dev_mode(tmp_path, monkeypatch):
    monkeypatch.delenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("SSPANEL_EXECUTOR_ENCRYPTION_KEYS", raising=False)
    monkeypatch.delenv("SSPANEL_EXECUTOR_ACTIVE_ENCRYPTION_KEY_ID", raising=False)
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


def test_storage_rewraps_rows_when_active_encryption_key_rotates(tmp_path, monkeypatch):
    path = tmp_path / "executor.sqlite3"
    monkeypatch.setenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", KEY)
    first = SQLiteJsonStore(str(path))
    first.table("jobs").insert({
        "jobId": "job-rotation",
        "idempotencyKey": "idem-rotation",
        "payload": {"sessionid": "rotation-secret"},
    })
    first.close()

    monkeypatch.delenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", raising=False)
    monkeypatch.setenv(
        "SSPANEL_EXECUTOR_ENCRYPTION_KEYS",
        json.dumps([
            {"id": "active-2026", "key": ROTATED_KEY},
            {"id": "legacy", "key": KEY},
        ]),
    )
    monkeypatch.setenv("SSPANEL_EXECUTOR_ACTIVE_ENCRYPTION_KEY_ID", "active-2026")
    rotated = SQLiteJsonStore(str(path))
    try:
        assert rotated.table("jobs").get(lambda row: row.get("jobId") == "job-rotation")["payload"]["sessionid"] == "rotation-secret"
    finally:
        rotated.close()

    assert b"v2:active-2026:" in path.read_bytes()

    monkeypatch.setenv(
        "SSPANEL_EXECUTOR_ENCRYPTION_KEYS",
        json.dumps([{"id": "active-2026", "key": ROTATED_KEY}]),
    )
    reopened = SQLiteJsonStore(str(path))
    try:
        assert reopened.table("jobs").get(lambda row: row.get("jobId") == "job-rotation")["payload"]["sessionid"] == "rotation-secret"
    finally:
        reopened.close()


def test_storage_reads_legacy_v1_rows_and_rewraps_them(tmp_path, monkeypatch):
    path = tmp_path / "executor.sqlite3"
    monkeypatch.setenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", KEY)
    empty = SQLiteJsonStore(str(path))
    empty.close()

    key = base64.urlsafe_b64decode(KEY)
    nonce = os.urandom(12)
    plaintext = json.dumps({"jobId": "legacy-v1", "idempotencyKey": "legacy-idem"}).encode("utf-8")
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, None)
    legacy_payload = "v1:" + base64.urlsafe_b64encode(nonce + ciphertext).decode("ascii")
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "INSERT INTO executor_rows(table_name, row_key, secondary_key, payload) VALUES (?, ?, ?, ?)",
            ("jobs", "legacy-v1", "legacy-idem", legacy_payload),
        )
        connection.commit()
    finally:
        connection.close()

    rotated = SQLiteJsonStore(str(path))
    try:
        assert rotated.table("jobs").get(lambda row: row.get("jobId") == "legacy-v1") is not None
    finally:
        rotated.close()
    assert b"v2:legacy:" in path.read_bytes()


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

    assert not (tmp_path / "executor.json.legacy.json").exists()
    assert not (tmp_path / "executor.json.legacy.enc").exists()
    assert b"legacy-secret" not in path.read_bytes()


def test_storage_removes_stale_plaintext_legacy_backup(tmp_path, monkeypatch):
    monkeypatch.setenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", KEY)
    path = tmp_path / "executor.sqlite3"
    store = SQLiteJsonStore(str(path))
    store.close()

    legacy_backup = tmp_path / "executor.sqlite3.legacy.json"
    legacy_backup.write_text(json.dumps({"accounts": {"1": {"sessionid": "stale-secret"}}}))

    reopened = SQLiteJsonStore(str(path))
    reopened.close()

    assert not legacy_backup.exists()
    assert b"stale-secret" not in path.read_bytes()
