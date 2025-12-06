"""Shared helpers for syncing between SQLite and YAML."""

from __future__ import annotations

import base64
import logging
import os
import textwrap
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import MutableMapping
from collections.abc import Sequence
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import naay
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.sql.schema import Column

DEFAULT_NAAY_VERSION = "1.0"
NULL_SENTINEL = "<:__NULL__:>"
BLOB_SENTINEL_BASE85 = "<:__BASE85__:>"
BLOB_SENTINEL_BASE64 = "<:__BASE64__:>"
BLOB_DEFAULT_SENTINEL = BLOB_SENTINEL_BASE85
BLOB_LINE_WIDTH = 64

_LOGGER = logging.getLogger(__name__)


class YamlSynchronizer:
    """Handle load/save cycles between SQLite tables and YAML files."""

    def __init__(
        self,
        metadata: MetaData,
        storage_path: Path | str,
        *,
        naay_version: str = DEFAULT_NAAY_VERSION,
        null_token: str = NULL_SENTINEL,
        write_workers: int | None = None,
        blob_encoding: str = BLOB_DEFAULT_SENTINEL,
    ) -> None:
        """
        Initialize the YAML synchronizer.

        Args:
            metadata: SQLAlchemy metadata containing table definitions.
            storage_path: Directory path where YAML files will be stored.
            naay_version: Version string for YAML format compatibility.
            null_token: Sentinel string used to represent NULL values in YAML.
            write_workers: Optional override for the number of concurrent write
                workers to use when persisting YAML files.
            blob_encoding: Sentinel that dictates whether blobs are stored as
                ASCII85 or Base64 text.

        """
        self.metadata = metadata
        self.storage_path = Path(storage_path)
        self.naay_version = naay_version
        self.null_token = null_token
        self._write_workers = self._resolve_worker_count(write_workers)
        self._blob_sentinel = blob_encoding
        self._blob_encoder = self._resolve_blob_encoder(blob_encoding)

    # ------------------------------------------------------------------
    # Public API
    def load(self, engine: Engine) -> None:
        """Push YAML contents into SQLite tables."""
        self.storage_path.mkdir(parents=True, exist_ok=True)
        tables = list(self.metadata.sorted_tables)
        table_rows = self._load_table_rows(tables)
        with engine.begin() as conn:
            for table, rows in table_rows:
                conn.execute(table.delete())
                if rows:
                    conn.execute(table.insert(), rows)

    def save(self, engine: Engine) -> None:
        """Flush SQLite tables out to YAML."""
        self.storage_path.mkdir(parents=True, exist_ok=True)
        table_payloads: list[tuple[Table, Sequence[Mapping[str, str]]]] = []
        with engine.begin() as conn:
            for table in self.metadata.sorted_tables:
                result = conn.execute(select(table))
                mappings = [dict(row) for row in result.mappings()]
                encoded_rows = [
                    self._encode_row(table, mapping) for mapping in mappings
                ]
                table_payloads.append((table, encoded_rows))
        self._flush_table_payloads(table_payloads)

    # ------------------------------------------------------------------
    # YAML IO helpers
    def _row_file(self, table: Table) -> Path:
        return self.storage_path / f"{table.name}.yaml"

    def _read_rows(self, table: Table) -> Sequence[MutableMapping[str, Any]]:
        path = self._row_file(table)
        if not path.exists():
            return []

        text = path.read_text(encoding="utf-8")
        payload = naay.loads(text)

        if not isinstance(payload, dict):
            msg = f"Expected dict for table {table.name}, got {type(payload)!r}"
            raise TypeError(
                msg,
            )

        version = payload.get("_naay_version")
        if version and version != self.naay_version:
            _LOGGER.warning(
                "Table %s stored with naay version %s (expected %s)",
                table.name,
                version,
                self.naay_version,
            )

        rows = payload.get("rows", [])
        if not isinstance(rows, list):
            msg = f"rows for table {table.name} must be a list, got {type(rows)!r}"
            raise TypeError(
                msg,
            )

        decoded: list[MutableMapping[str, Any]] = []
        for index, raw_row in enumerate(rows):
            if not isinstance(raw_row, dict):
                msg = f"row {index} for table {table.name} is not a mapping"
                raise TypeError(msg)
            decoded.append(self._decode_row(table, raw_row))
        return decoded

    def _write_rows(self, table: Table, rows: Sequence[Mapping[str, str]]) -> None:
        payload: dict[str, Any] = {
            "_naay_version": self.naay_version,
            "rows": list(rows),
        }
        text = naay.dumps(payload)
        self._row_file(table).write_text(text, encoding="utf-8")

    def _load_table_rows(
        self,
        tables: Sequence[Table],
    ) -> Sequence[tuple[Table, Sequence[MutableMapping[str, Any]]]]:
        if not tables:
            return []

        if self._write_workers > 1:
            parallel = self._read_with_executor(tables)
            if parallel is not None:
                return parallel

        return [(table, self._read_rows(table)) for table in tables]

    def _read_with_executor(
        self,
        tables: Sequence[Table],
    ) -> Sequence[tuple[Table, Sequence[MutableMapping[str, Any]]]] | None:
        try:
            with ThreadPoolExecutor(
                max_workers=self._write_workers,
            ) as executor:
                futures: list[tuple[Table, Future[Sequence[MutableMapping[str, Any]]]]]
                futures = [
                    (table, executor.submit(self._read_rows, table)) for table in tables
                ]
                return [(table, future.result()) for table, future in futures]
        except RuntimeError as exc:
            if not self._executor_unavailable(exc):
                raise
            _LOGGER.debug(
                "ThreadPoolExecutor unavailable during interpreter shutdown; "
                "falling back to synchronous reads",
                exc_info=_LOGGER.isEnabledFor(logging.DEBUG),
            )
        return None

    def _flush_table_payloads(
        self,
        table_payloads: Sequence[tuple[Table, Sequence[Mapping[str, str]]]],
    ) -> None:
        if not table_payloads:
            return

        if self._write_workers <= 1:
            for table, rows in table_payloads:
                self._write_rows(table, rows)
            return

        if self._flush_with_executor(table_payloads):
            return

        for table, rows in table_payloads:
            self._write_rows(table, rows)

    def _flush_with_executor(
        self,
        table_payloads: Sequence[tuple[Table, Sequence[Mapping[str, str]]]],
    ) -> bool:
        try:
            with ThreadPoolExecutor(
                max_workers=self._write_workers,
            ) as executor:
                futures: list[Future[None]] = []
                for table, rows in table_payloads:
                    futures.append(
                        executor.submit(self._write_rows, table, rows),
                    )
                for future in futures:
                    future.result()
        except RuntimeError as exc:
            if not self._executor_unavailable(exc):
                raise
            _LOGGER.debug(
                "ThreadPoolExecutor unavailable during interpreter shutdown; "
                "falling back to synchronous writes",
                exc_info=_LOGGER.isEnabledFor(logging.DEBUG),
            )
            return False
        return True

    @staticmethod
    def _executor_unavailable(exc: BaseException) -> bool:
        return "interpreter shutdown" in str(exc).lower()

    def _resolve_worker_count(self, write_workers: int | None) -> int:
        if write_workers is None:
            cpu_count = os.cpu_count() or 1
            return min(32, cpu_count * 4)
        if write_workers < 1:
            msg = "write_workers must be at least 1"
            raise ValueError(msg)
        return write_workers

    def _resolve_blob_encoder(self, blob_encoding: str) -> Callable[[bytes], bytes]:
        if blob_encoding == BLOB_SENTINEL_BASE85:
            return base64.a85encode
        if blob_encoding == BLOB_SENTINEL_BASE64:
            return base64.b64encode
        msg = "blob_encoding must be either <:__BASE85__:> or <:__BASE64__:>"
        raise ValueError(msg)

    # ------------------------------------------------------------------
    # Encoding/decoding helpers
    def _decode_row(
        self,
        table: Table,
        raw_row: Mapping[str, Any],
    ) -> MutableMapping[str, Any]:
        decoded: dict[str, Any] = {}
        for column in table.columns:
            if column.name not in raw_row:
                continue
            decoded[column.name] = self._decode_value(column, raw_row[column.name])
        return decoded

    def _encode_row(
        self,
        table: Table,
        row: Mapping[str, Any],
    ) -> MutableMapping[str, str]:
        encoded: dict[str, str] = {}
        for column in table.columns:
            encoded[column.name] = self._encode_value(row.get(column.name))
        return encoded

    def _decode_value(self, column: Column[Any], text: Any) -> Any:  # noqa: PLR0911
        if text is None or text == self.null_token:
            return None

        python_type = getattr(column.type, "python_type", None)
        if python_type is None:
            return text

        if python_type is bytes:
            return self._decode_blob(text)

        if python_type is bool:
            normalized = text.strip().lower()
            if normalized in {"1", "true", "t", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "f", "no", "n", "off"}:
                return False
            msg = f"Cannot coerce value {text!r} into bool for column {column.name}"
            raise ValueError(
                msg,
            )

        try:
            return python_type(text)
        except (ValueError, TypeError):
            return text

    def _encode_value(self, value: Any) -> str:
        if value is None:
            return self.null_token
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (bytes, bytearray, memoryview)):
            if isinstance(value, bytes):
                raw = value
            elif isinstance(value, memoryview):
                raw = value.tobytes()
            else:
                raw = bytes(value)
            payload = self._blob_encoder(raw).decode("ascii")
            chunked = (
                "\n".join(textwrap.wrap(payload, width=BLOB_LINE_WIDTH))
                if payload
                else ""
            )
            if chunked:
                return f"{self._blob_sentinel}\n{chunked}"
            return self._blob_sentinel
        return str(value)

    def _decode_blob(self, text: str | None) -> bytes:
        if not isinstance(text, str):
            msg = "BLOB columns must use the encoded payload markers"
            raise ValueError(msg)

        def _decode_payload(
            sentinel: str,
            decoder: Callable[[bytes], bytes],
        ) -> bytes | None:
            if not text.startswith(sentinel):
                return None
            payload = text[len(sentinel) :].lstrip("\n")
            normalized = payload.replace("\n", "")
            if not normalized:
                return b""
            return decoder(normalized.encode("ascii"))

        decoded = _decode_payload(BLOB_SENTINEL_BASE85, base64.a85decode)
        if decoded is not None:
            return decoded

        decoded = _decode_payload(BLOB_SENTINEL_BASE64, base64.b64decode)
        if decoded is not None:
            return decoded

        msg = "BLOB columns must use either <:__BASE85__:> or <:__BASE64__:> encodings"
        raise ValueError(msg)
