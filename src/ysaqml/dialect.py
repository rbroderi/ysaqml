"""SQLAlchemy dialect and engine plugin for ysaqml."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from sqlalchemy.dialects.sqlite.pysqlite import SQLiteDialect_pysqlite
from sqlalchemy.engine import URL
from sqlalchemy.engine import CreateEnginePlugin
from sqlalchemy.engine import Engine
from sqlalchemy.schema import MetaData

from .sync import BLOB_DEFAULT_SENTINEL
from .sync import DEFAULT_NAAY_VERSION
from .sync import NULL_SENTINEL
from .sync import YamlSynchronizer


class YamlDialect(SQLiteDialect_pysqlite):
    """Thin wrapper around SQLite so SQLAlchemy recognizes the ``ysaqml`` scheme."""

    name = "ysaqml"
    supports_statement_cache = True


class YamlSyncPlugin(CreateEnginePlugin):
    """Engine plugin that keeps SQLite rows in sync with YAML storage."""

    name = "ysaqml.sync"

    def __init__(self, url: URL, kwargs: dict[str, Any]) -> None:
        """Initialize the YAML sync plugin with metadata and storage configuration."""
        metadata_obj = kwargs.pop("metadata", None)
        if metadata_obj is None:
            metadata_obj = _metadata_from_query(url)
        self._metadata = _ensure_metadata(metadata_obj)

        storage_path = kwargs.pop("storage_path", None) or url.database or "."
        naay_version = (
            kwargs.pop("naay_version", None)
            or url.query.get("naay_version")
            or DEFAULT_NAAY_VERSION
        )
        null_token = (
            kwargs.pop("null_token", None)
            or url.query.get("null_token")
            or NULL_SENTINEL
        )
        blob_encoding = (
            kwargs.pop("blob_encoding", None)
            or url.query.get("blob_encoding")
            or BLOB_DEFAULT_SENTINEL
        )

        naay_version = str(naay_version)
        null_token = str(null_token)
        if isinstance(blob_encoding, tuple):
            blob_encoding = blob_encoding[0]
        blob_encoding = str(blob_encoding)
        self._sync = YamlSynchronizer(
            self._metadata,
            Path(storage_path),
            naay_version=naay_version,
            null_token=null_token,
            blob_encoding=blob_encoding,
        )

    def update_url(self, url: URL) -> URL:
        """Return the URL unchanged."""
        return url

    def handle_dialect_kwargs(
        self,
        dialect_cls: type,  # noqa: ARG002
        dialect_args: dict[str, Any],
    ) -> None:
        """Remove custom plugin arguments from dialect kwargs."""
        dialect_args.pop("metadata", None)
        dialect_args.pop("storage_path", None)
        dialect_args.pop("naay_version", None)
        dialect_args.pop("null_token", None)
        dialect_args.pop("blob_encoding", None)

    def engine_created(self, engine: Engine) -> None:
        """Create database tables, load YAML data, and register disposal hook."""
        self._sync.metadata.create_all(engine)
        self._sync.load(engine)

        original_dispose = engine.dispose

        def _dispose(*args: Any, **kwargs: Any) -> None:
            self._sync.save(engine)
            original_dispose(*args, **kwargs)

        engine.dispose = _dispose  # type: ignore[assignment]

    def _save_on_dispose(self, engine: Engine) -> None:
        self._sync.save(engine)


def _metadata_from_query(url: URL) -> MetaData | None:
    spec = url.query.get("metadata")
    if not spec:
        return None
    if isinstance(spec, tuple):
        spec = spec[0]
    return _import_metadata(spec)


def _import_metadata(spec: str) -> MetaData:
    module_name, sep, attribute = spec.partition(":")
    if not sep:
        msg = f"metadata spec must be of the form 'module:attribute', got {spec!r}"
        raise ValueError(
            msg,
        )
    module = importlib.import_module(module_name)
    try:
        value = getattr(module, attribute)
    except AttributeError as exc:  # pragma: no cover - defensive branch
        msg = f"module {module_name!r} has no attribute {attribute!r}"
        raise AttributeError(
            msg,
        ) from exc
    return _ensure_metadata(value)


def _ensure_metadata(value: Any | None) -> MetaData:
    if isinstance(value, MetaData):
        return value
    if value is None:
        msg = "ysaqml requires a sqlalchemy.MetaData instance"
        raise ValueError(msg)
    msg = f"metadata must be a sqlalchemy.MetaData instance, got {type(value)!r}"
    raise TypeError(
        msg,
    )
