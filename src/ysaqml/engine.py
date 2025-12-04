"""YAML-backed SQLAlchemy engine built on in-memory SQLite."""

from __future__ import annotations

import logging
from collections.abc import Mapping, MutableMapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import naay
from sqlalchemy import MetaData, Table, create_engine, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.sql.schema import Column

DEFAULT_NAAY_VERSION = "2025.12.03-0"
NULL_SENTINEL = "__NULL__"

_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class YamlSqliteEngine:
    """Context manager that syncs SQLite <-> YAML files via naay."""

    metadata: MetaData
    storage_path: Path | str
    naay_version: str = DEFAULT_NAAY_VERSION
    null_token: str = NULL_SENTINEL
    engine: Engine | None = field(init=False, default=None, repr=False)
    _event_suppression: int = field(init=False, default=0, repr=False)
    _events_enabled: bool = field(init=False, default=False, repr=False)

    def __post_init__(self) -> None:
        self.storage_path = Path(self.storage_path)
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        event.listen(self.engine, "commit", self._handle_commit)
        event.listen(self.engine, "rollback", self._handle_rollback)

    def __enter__(self) -> YamlSqliteEngine:
        with self._suspend_events():
            self.metadata.create_all(self.engine)
        self.load()
        self._events_enabled = True
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            self.save()
        self._events_enabled = False
        assert self.engine is not None
        self.engine.dispose()
        return False

    # ---------------------------------------------------------------------
    # Public API
    def load(self) -> None:
        """Clear SQLite tables and hydrate them from all YAML files."""

        self.storage_path.mkdir(parents=True, exist_ok=True)
        assert self.engine is not None
        with self._suspend_events():
            with self.engine.begin() as conn:
                for table in self.metadata.sorted_tables:
                    rows = self._read_rows(table)
                    conn.execute(table.delete())
                    if rows:
                        conn.execute(table.insert(), rows)

    def save(self) -> None:
        """Dump every SQLite table into its companion YAML file."""

        self.storage_path.mkdir(parents=True, exist_ok=True)
        assert self.engine is not None
        with self._suspend_events():
            with self.engine.begin() as conn:
                for table in self.metadata.sorted_tables:
                    result = conn.execute(select(table))
                    mappings = [dict(row._mapping) for row in result]
                    encoded_rows = [
                        self._encode_row(table, mapping) for mapping in mappings
                    ]
                    self._write_rows(table, encoded_rows)

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

        version = payload.get("_naay_version")
        if version and version != self.naay_version:
            _LOGGER.warning(
                "Table %%s stored with naay version %%s (expected %%s)",
                table.name,
                version,
                self.naay_version,
            )

        rows = payload.get("rows", [])
        if not isinstance(rows, list):
            raise TypeError(
                f"rows for table {table.name} must be a list, got {type(rows)!r}"
            )

        decoded: list[MutableMapping[str, Any]] = []
        for index, raw_row in enumerate(rows):
            if not isinstance(raw_row, dict):
                raise TypeError(f"row {index} for table {table.name} is not a mapping")
            decoded.append(self._decode_row(table, raw_row))
        return decoded

    def _write_rows(self, table: Table, rows: Sequence[Mapping[str, str]]) -> None:
        payload = {
            "_naay_version": self.naay_version,
            "rows": list(rows),
        }
        text = naay.dumps(payload)
        self._row_file(table).write_text(text, encoding="utf-8")

    # ------------------------------------------------------------------
    # Encoding/decoding helpers
    def _decode_row(
        self, table: Table, raw_row: Mapping[str, str]
    ) -> MutableMapping[str, Any]:
        decoded: dict[str, Any] = {}
        for column in table.columns:
            if column.name not in raw_row:
                continue
            decoded[column.name] = self._decode_value(column, raw_row[column.name])
        return decoded

    def _encode_row(
        self, table: Table, row: Mapping[str, Any]
    ) -> MutableMapping[str, str]:
        encoded: dict[str, str] = {}
        for column in table.columns:
            encoded[column.name] = self._encode_value(row.get(column.name))
        return encoded

    def _decode_value(self, column: Column[Any], text: str | None) -> Any:
        if text is None or text == self.null_token:
            return None

        python_type = getattr(column.type, "python_type", None)
        if python_type is None:
            return text

        if python_type is bool:
            normalized = text.strip().lower()
            if normalized in {"1", "true", "t", "yes", "y", "on"}:
                return True
            if normalized in {"0", "false", "f", "no", "n", "off"}:
                return False
            raise ValueError(
                f"Cannot coerce value {text!r} into bool for column {column.name}"
            )

        try:
            return python_type(text)
        except Exception:  # pragma: no cover - fallback path
            return text

    def _encode_value(self, value: Any) -> str:
        if value is None:
            return self.null_token
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    # ------------------------------------------------------------------
    # Engine event wiring
    def _handle_commit(self, connection) -> None:
        if not self._events_enabled or self._event_suppression:
            return
        self.save()

    def _handle_rollback(self, connection) -> None:
        if not self._events_enabled or self._event_suppression:
            return
        self.load()

    @contextmanager
    def _suspend_events(self):
        self._event_suppression += 1
        try:
            yield
        finally:
            self._event_suppression -= 1
