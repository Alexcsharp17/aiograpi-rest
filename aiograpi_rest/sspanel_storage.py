"""Encrypted SQLite storage primitives for the SS-panel executor.

The executor keeps its public store methods deliberately small so workflow code
does not depend on the persistence implementation. Rows are encrypted as JSON
payloads; only lookup keys remain in SQLite indexes.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import importlib
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class StorageConfigurationError(RuntimeError):
    """Raised when encrypted executor storage is not configured safely."""


class SecretProvider(Protocol):
    """Deployment-owned boundary for resolving executor secrets.

    The executor only needs a named secret. A deployment may inject a provider
    backed by Vault, AWS Secrets Manager, or Kubernetes without making the
    platform runtime depend on that vendor SDK.
    """

    def get(self, name: str) -> str:
        ...


class EnvironmentSecretProvider:
    """Resolve secrets from ``NAME_FILE`` first, then ``NAME``.

    This is the default provider for local development and Docker/Kubernetes
    mounted secrets. Remote providers should implement ``SecretProvider`` and
    be passed into ``SQLiteJsonStore`` by the deployment adapter.
    """

    def get(self, name: str) -> str:
        file_path = os.getenv(f"{name}_FILE", "").strip()
        if file_path:
            try:
                value = Path(file_path).read_text(encoding="utf-8").strip()
            except (OSError, UnicodeError) as error:
                raise StorageConfigurationError(f"{name}_FILE could not be read") from error
            if not value:
                raise StorageConfigurationError(f"{name}_FILE must not be empty")
            return value
        return os.getenv(name, "").strip()


_default_secret_provider: SecretProvider = EnvironmentSecretProvider()


def set_default_secret_provider(provider: SecretProvider) -> None:
    """Install a deployment-owned provider before the app starts serving."""
    if not callable(getattr(provider, "get", None)):
        raise StorageConfigurationError("Configured secret provider must implement get(name)")
    global _default_secret_provider
    _default_secret_provider = provider


def resolve_secret_provider() -> SecretProvider:
    """Resolve an optional deployment adapter without bundling a vendor SDK.

    ``SSPANEL_SECRET_PROVIDER_CLASS`` accepts ``module.path:ProviderClass``.
    The provider is constructed once during application startup and must expose
    ``get(name)``. With no class configured, mounted files and environment
    variables remain the default.
    """
    specification = os.getenv("SSPANEL_SECRET_PROVIDER_CLASS", "").strip()
    if not specification:
        return EnvironmentSecretProvider()
    module_name, separator, class_name = specification.partition(":")
    if not separator or not module_name or not class_name:
        raise StorageConfigurationError(
            "SSPANEL_SECRET_PROVIDER_CLASS must use module.path:ProviderClass format"
        )
    try:
        module = importlib.import_module(module_name)
        provider_factory = getattr(module, class_name)
        provider = provider_factory()
    except (ImportError, AttributeError, TypeError) as error:
        raise StorageConfigurationError("Configured secret provider could not be loaded") from error
    set_default_secret_provider(provider)
    return provider


def read_secret(name: str, provider: Optional[SecretProvider] = None) -> str:
    """Read a secret through the configured deployment boundary."""
    return (provider or _default_secret_provider).get(name)


def _read_secret(name: str, provider: Optional[SecretProvider]) -> str:
    """Keep secret resolution injectable while preserving the legacy helper."""
    return read_secret(name, provider)


def _decode_encryption_key(encoded: object, label: str) -> bytes:
    if not isinstance(encoded, str) or not encoded.strip():
        raise StorageConfigurationError(f"{label} must be configured")
    try:
        value = encoded.strip()
        padding = "=" * (-len(value) % 4)
        key = base64.urlsafe_b64decode((value + padding).encode("ascii"))
    except (ValueError, UnicodeError) as error:
        raise StorageConfigurationError(f"{label} must be urlsafe base64") from error
    if len(key) != 32:
        raise StorageConfigurationError(f"{label} must decode to 32 bytes")
    return key


def _encryption_keyring(provider: Optional[SecretProvider] = None) -> tuple[dict[str, bytes], str]:
    """Resolve the active storage key and optional previous rotation keys."""
    configured = _read_secret("SSPANEL_EXECUTOR_ENCRYPTION_KEYS", provider)
    if configured:
        try:
            parsed = json.loads(configured)
        except json.JSONDecodeError as error:
            raise StorageConfigurationError(
                "SSPANEL_EXECUTOR_ENCRYPTION_KEYS must be valid JSON"
            ) from error

        entries = parsed.get("keys") if isinstance(parsed, dict) else parsed
        if not isinstance(entries, list) or not entries:
            raise StorageConfigurationError(
                "SSPANEL_EXECUTOR_ENCRYPTION_KEYS must be a non-empty JSON list"
            )

        keys: dict[str, bytes] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            key_id = entry.get("id")
            if not isinstance(key_id, str) or not key_id.strip():
                continue
            key_id = key_id.strip()
            if key_id in keys:
                raise StorageConfigurationError(
                    "SSPANEL_EXECUTOR_ENCRYPTION_KEYS contains duplicate key ids"
                )
            keys[key_id] = _decode_encryption_key(
                entry.get("key"),
                f"SSPANEL_EXECUTOR_ENCRYPTION_KEYS[{key_id}].key",
            )

        if not keys:
            raise StorageConfigurationError(
                "SSPANEL_EXECUTOR_ENCRYPTION_KEYS does not contain a valid key"
            )
        active_id = _read_secret("SSPANEL_EXECUTOR_ACTIVE_ENCRYPTION_KEY_ID", provider)
        if not active_id:
            active_id = next(iter(keys))
        if active_id not in keys:
            raise StorageConfigurationError(
                "SSPANEL_EXECUTOR_ACTIVE_ENCRYPTION_KEY_ID must reference a configured key"
            )
        return keys, active_id

    encoded = _read_secret("SSPANEL_EXECUTOR_ENCRYPTION_KEY", provider)
    if encoded:
        return {"legacy": _decode_encryption_key(encoded, "SSPANEL_EXECUTOR_ENCRYPTION_KEY")}, "legacy"
    if os.getenv("SSPANEL_EXECUTOR_ALLOW_INSECURE_DEV_STORAGE", "").lower() == "true":
        return {
            "insecure-dev": hashlib.sha256(b"sspanel-executor-insecure-development-key").digest()
        }, "insecure-dev"
    raise StorageConfigurationError(
        "SSPANEL_EXECUTOR_ENCRYPTION_KEY or SSPANEL_EXECUTOR_ENCRYPTION_KEYS must be configured for executor storage"
    )


class _JsonCodec:
    def __init__(self, keys: dict[str, bytes], active_key_id: str):
        self._keys = {key_id: AESGCM(key) for key_id, key in keys.items()}
        self.active_key_id = active_key_id

    def encode(self, value: dict[str, Any]) -> str:
        nonce = os.urandom(12)
        plaintext = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        ciphertext = self._keys[self.active_key_id].encrypt(nonce, plaintext, None)
        encoded = base64.urlsafe_b64encode(nonce + ciphertext).decode("ascii")
        return f"v2:{self.active_key_id}:{encoded}"

    def decode_with_metadata(self, value: str) -> tuple[dict[str, Any], str, str]:
        if value.startswith("v2:"):
            try:
                _, key_id, encoded = value.split(":", 2)
            except ValueError as error:
                raise StorageConfigurationError(
                    "Executor storage payload has an invalid encryption envelope"
                ) from error
            candidates = [(key_id, self._keys.get(key_id))]
            version = "v2"
        elif value.startswith("v1:"):
            encoded = value[3:]
            candidates = [(key_id, codec) for key_id, codec in self._keys.items()]
            version = "v1"
        else:
            raise StorageConfigurationError("Executor storage contains an unsupported encryption version")

        if not candidates or all(codec is None for _, codec in candidates):
            raise StorageConfigurationError("Executor storage payload references an unavailable encryption key")

        last_error: Optional[Exception] = None
        for key_id, codec in candidates:
            if codec is None:
                continue
            try:
                raw = base64.urlsafe_b64decode(encoded.encode("ascii"))
                if len(raw) <= 12:
                    raise ValueError("encrypted payload is too short")
                plaintext = codec.decrypt(raw[:12], raw[12:], None)
                decoded = json.loads(plaintext.decode("utf-8"))
                if not isinstance(decoded, dict):
                    raise StorageConfigurationError("Executor storage payload must be a JSON object")
                return decoded, key_id, version
            except (binascii.Error, InvalidTag, ValueError, UnicodeError, json.JSONDecodeError) as error:
                last_error = error

        raise StorageConfigurationError("Executor storage payload could not be decrypted") from last_error

    def decode(self, value: str) -> dict[str, Any]:
        decoded, _, _ = self.decode_with_metadata(value)
        return decoded

    def needs_rewrap(self, value: str) -> bool:
        return not value.startswith(f"v2:{self.active_key_id}:")


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
    def __init__(self, path: str, secret_provider: Optional[SecretProvider] = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        encryption_keys, active_key_id = _encryption_keyring(secret_provider)
        self.codec = _JsonCodec(encryption_keys, active_key_id)
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
        """Validate rows and re-wrap them with the active key during rotation."""
        rows = self.connection.execute("SELECT id, payload FROM executor_rows").fetchall()
        rewrapped: list[tuple[int, str]] = []
        for row_id, payload in rows:
            decoded, _, _ = self.codec.decode_with_metadata(payload)
            if self.codec.needs_rewrap(payload):
                rewrapped.append((int(row_id), self.codec.encode(decoded)))

        if not rewrapped:
            return
        with self.transaction():
            for row_id, payload in rewrapped:
                self.connection.execute(
                    "UPDATE executor_rows SET payload = ? WHERE id = ?",
                    (payload, row_id),
                )
