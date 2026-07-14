"""Encrypted SQLite storage primitives for the SS-panel executor.

The executor keeps its public store methods deliberately small so workflow code
does not depend on the persistence implementation. Rows are encrypted as JSON
payloads; only lookup keys remain in SQLite indexes.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class StorageConfigurationError(RuntimeError):
    """Raised when encrypted executor storage is not configured safely."""


def _encryption_key() -> bytes:
    encoded = os.getenv("SSPANEL_EXECUTOR_ENCRYPTION_KEY", "").strip()
    if not encoded:
        if os.getenv("SSPANEL_EXECUTOR_ALLOW_INSECURE_DEV_STORAGE", "").lower() == "true":
            return hashlib.sha256(b"sspanel-executor-insecure-development-key").digest()
        raise StorageConfigurationError(
            "SSPANEL_EXECUTOR_ENCRYPTION_KEY must be configured for executor storage"
        )

    try:
        padding = "=" * (-len(encoded) % 4)
        key = base64.urlsafe_b64decode((encoded + padding).encode("ascii"))
    except (ValueError, UnicodeError) as error:
        raise StorageConfigurationError("SSPANEL_EXECUTOR_ENCRYPTION_KEY must be urlsafe base64") from error
    if len(key) != 32:
        raise StorageConfigurationError("SSPANEL_EXECUTOR_ENCRYPTION_KEY must decode to 32 bytes")
    return key


class _JsonCodec:
    def __init__(self, key: bytes):
        self._aes = AESGCM(key)

    def encode(self, value: dict[str, Any]) -> str:
        nonce = os.urandom(12)
        plaintext = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ciphertext = self._aes.encrypt(nonce, plaintext, None)
        return "v1:" + base64.urlsafe_b64encode(nonce + ciphertext).decode("ascii")

    def decode(self, value: str) -> dict[str, Any]:
        if not value.startswith("v1:"):
            raise StorageConfigurationError("Executor storage contains an unsupported encryption version")
        try:
            raw = base64.urlsafe_b64decode(value[3:].encode("ascii"))
            plaintext = self._aes.decrypt(raw[:12], raw[12:], None)
            decoded = json.loads(plaintext.decode("utf-8"))
        except (binascii.Error, InvalidTag, ValueError, UnicodeError, json.JSONDecodeError) as error:
            raise StorageConfigurationError("Executor storage payload could not be decrypted") from error
        if not isinstance(decoded, dict):
            raise StorageConfigurationError("Executor storage payload must be a JSON object")
        return decoded


class _Field:
    def __init__(self, name: str):
        self.name = name

    def __eq__(self, expected: object) -> Callable[[dict[str, Any]], bool]:  # type: ignore[override]
        return lambda row: row.get(self.name) == expected


class Query:
    """TinyDB-compatible equality query used by the executor store."""

    def __getattr__(self, name: str) -> _Field:
        return _Field(name)


Predicate = Callable[[dict[str, Any]], bool]


class SQLiteJsonTable:
    def __init__(self, store: "SQLiteJsonStore", name: str, key_field: str, secondary_field: Optional[str] = None):
        self.store = store
        self.name = name
        self.key_field = key_field
        self.secondary_field = secondary_field

    def all(self) -> list[dict[str, Any]]:
        with self.store.lock:
            rows = self.store.connection.execute(
                "SELECT row_key, payload FROM executor_rows WHERE table_name = ? ORDER BY rowid",
                (self.name,),
            ).fetchall()
            return [self.store.decode_payload(payload) for _, payload in rows]

    def search(self, predicate: Predicate) -> list[dict[str, Any]]:
        return [row for row in self.all() if predicate(row)]

    def get(self, predicate: Predicate) -> Optional[dict[str, Any]]:
        for row in self.all():
            if predicate(row):
                return row
        return None

    def insert(self, row: dict[str, Any]) -> None:
        with self.store.transaction():
            self.insert_in_transaction(row)

    def insert_in_transaction(self, row: dict[str, Any]) -> None:
        row_key, secondary_key = self._keys(row)
        payload = self.store.encode_payload(row)
        self.store.connection.execute(
            "INSERT INTO executor_rows(table_name, row_key, secondary_key, payload) VALUES (?, ?, ?, ?)",
            (self.name, row_key, secondary_key, payload),
        )

    def update(self, updates: dict[str, Any], predicate: Predicate) -> int:
        with self.store.transaction():
            return self.update_in_transaction(updates, predicate)

    def update_in_transaction(self, updates: dict[str, Any], predicate: Predicate) -> int:
        rows = self.rows_with_ids()
        changed = 0
        for row_id, row in rows:
            if not predicate(row):
                continue
            merged = {**row, **updates}
            row_key, secondary_key = self._keys(merged)
            self.replace_row_in_transaction(row_id, merged, row_key=row_key, secondary_key=secondary_key)
            changed += 1
        return changed

    def upsert(self, row: dict[str, Any], predicate: Predicate) -> None:
        existing = self.get(predicate)
        if existing:
            self.update(row, predicate)
            return
        self.insert(row)

    def rows_with_ids(self) -> list[tuple[int, dict[str, Any]]]:
        rows = self.store.connection.execute(
            "SELECT id, payload FROM executor_rows WHERE table_name = ? ORDER BY id",
            (self.name,),
        ).fetchall()
        return [(int(row_id), self.store.decode_payload(payload)) for row_id, payload in rows]

    def replace_row_in_transaction(
        self,
        row_id: int,
        row: dict[str, Any],
        *,
        row_key: Optional[str] = None,
        secondary_key: Optional[str] = None,
    ) -> None:
        resolved_row_key, resolved_secondary_key = self._keys(row)
        self.store.connection.execute(
            "UPDATE executor_rows SET row_key = ?, secondary_key = ?, payload = ? WHERE id = ?",
            (
                row_key or resolved_row_key,
                secondary_key if secondary_key is not None else resolved_secondary_key,
                self.store.encode_payload(row),
                row_id,
            ),
        )

    def _keys(self, row: dict[str, Any]) -> tuple[str, Optional[str]]:
        value = row.get(self.key_field)
        if not isinstance(value, str) or not value:
            raise ValueError(f"{self.name}.{self.key_field} must be a non-empty string")
        secondary = row.get(self.secondary_field) if self.secondary_field else None
        if secondary is not None and not isinstance(secondary, str):
            secondary = str(secondary)
        return value, secondary


class SQLiteJsonStore:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.codec = _JsonCodec(_encryption_key())
        self._migrate_legacy_json()
        self.connection = sqlite3.connect(str(self.path), timeout=30, check_same_thread=False)
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 30000")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS executor_rows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                row_key TEXT NOT NULL,
                secondary_key TEXT,
                payload TEXT NOT NULL,
                UNIQUE(table_name, row_key),
                UNIQUE(table_name, secondary_key)
            )
            """
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS executor_rows_table_idx ON executor_rows(table_name)"
        )
        self.connection.commit()
        self._imported_legacy_rows = self._legacy_rows
        self._legacy_rows = None
        self._import_legacy_rows()
        self._validate_existing_payloads()

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    @contextmanager
    def transaction(self) -> Iterator[None]:
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                yield
            except Exception:
                self.connection.rollback()
                raise
            else:
                self.connection.commit()

    def table(self, name: str) -> SQLiteJsonTable:
        definitions = {
            "jobs": ("jobId", "idempotencyKey"),
            "accounts": ("executorAccountId", None),
            "usage": ("usageKey", None),
            "outbox": ("outboxId", None),
        }
        key_field, secondary_field = definitions[name]
        return SQLiteJsonTable(self, name, key_field, secondary_field)

    def encode_payload(self, row: dict[str, Any]) -> str:
        return self.codec.encode(row)

    def decode_payload(self, payload: str) -> dict[str, Any]:
        return self.codec.decode(payload)

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    def _migrate_legacy_json(self) -> None:
        self._legacy_rows: Optional[dict[str, list[dict[str, Any]]]] = None
        if not self.path.exists() or self.path.stat().st_size == 0:
            return
        with self.path.open("rb") as stream:
            header = stream.read(16)
        if header.startswith(b"SQLite format 3"):
            return
        try:
            raw = json.loads(self.path.read_text())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise StorageConfigurationError("Executor storage is neither SQLite nor valid legacy JSON") from error
        self._legacy_rows = {
            table: [row for row in rows.values() if isinstance(row, dict)]
            for table, rows in raw.items()
            if table in {"jobs", "accounts", "usage"} and isinstance(rows, dict)
        }
        legacy_path = self.path.with_name(self.path.name + ".legacy.json")
        if legacy_path.exists():
            legacy_path.unlink()
        self.path.replace(legacy_path)

    def _import_legacy_rows(self) -> None:
        if not self._imported_legacy_rows:
            return
        for table_name, rows in self._imported_legacy_rows.items():
            table = self.table(table_name)
            for row in rows:
                try:
                    table.insert(row)
                except sqlite3.IntegrityError:
                    continue

    def _validate_existing_payloads(self) -> None:
        """Fail during startup when the configured key cannot read persisted rows."""
        rows = self.connection.execute("SELECT payload FROM executor_rows").fetchall()
        for (payload,) in rows:
            self.decode_payload(payload)
