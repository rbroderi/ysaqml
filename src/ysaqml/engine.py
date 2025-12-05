"""YAML-backed SQLAlchemy engine built on in-memory SQLite."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import Literal
from typing import Self

from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

from .dialect import YamlSyncPlugin
from .sync import DEFAULT_NAAY_VERSION
from .sync import NULL_SENTINEL
from .sync import YamlSynchronizer


@dataclass(slots=True)
class YamlSqliteEngine:
    """Context manager that syncs SQLite <-> YAML files via naay."""

    metadata: MetaData
    storage_path: Path | str
    naay_version: str = DEFAULT_NAAY_VERSION
    null_token: str = NULL_SENTINEL
    write_workers: int | None = None
    engine: Engine | None = field(init=False, default=None, repr=False)
    _sync: YamlSynchronizer = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Initialize the engine and synchronizer after dataclass initialization."""
        self.storage_path = Path(self.storage_path)
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        self._sync = YamlSynchronizer(
            self.metadata,
            self.storage_path,
            naay_version=self.naay_version,
            null_token=self.null_token,
            write_workers=self.write_workers,
        )

    def __enter__(self) -> Self:
        """Enter context manager, creating tables and loading data from YAML files."""
        if self.engine is None:
            msg = "YamlSqliteEngine has already been closed"
            raise RuntimeError(msg)
        self.metadata.create_all(self.engine)
        self.load()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        """Clean up resources and save data if no exception occurred."""
        if exc_type is None:
            self.save()
        assert self.engine is not None
        self.engine.dispose()
        return False

    # ---------------------------------------------------------------------
    # Public API
    def load(self) -> None:
        """Clear SQLite tables and hydrate them from all YAML files."""
        assert self.engine is not None
        self._sync.load(self.engine)

    def save(self) -> None:
        """Dump every SQLite table into its companion YAML file."""
        assert self.engine is not None
        self._sync.save(self.engine)


def create_yaml_engine(
    metadata: MetaData,
    storage_path: Path | str,
    *,
    naay_version: str = DEFAULT_NAAY_VERSION,
    null_token: str = NULL_SENTINEL,
    plugins: Sequence[Any] | None = None,
    **engine_kwargs: Any,
) -> Engine:
    """Create a SQLAlchemy engine configured for the ``ysaqml`` dialect."""
    storage = Path(storage_path)
    url = URL.create("ysaqml", database=":memory:")

    plugin_list = list(plugins or [])

    def _plugin_identifier(value: Any) -> str:
        if isinstance(value, str):
            return value
        name = getattr(value, "name", None)
        if not name:
            msg = "plugins must be strings or CreateEnginePlugin classes with a 'name'"
            raise TypeError(
                msg,
            )
        return name

    plugin_names = [_plugin_identifier(item) for item in plugin_list]
    if "ysaqml.sync" not in plugin_names:
        plugin_names.append(YamlSyncPlugin.name)

    engine_kwargs.setdefault("future", True)
    engine_kwargs.setdefault("storage_path", str(storage))
    engine_kwargs.setdefault("poolclass", StaticPool)

    connect_args = dict(engine_kwargs.get("connect_args") or {})
    connect_args.setdefault("check_same_thread", False)
    engine_kwargs["connect_args"] = connect_args
    return create_engine(
        url,
        metadata=metadata,
        plugins=plugin_names,
        naay_version=naay_version,
        null_token=null_token,
        **engine_kwargs,
    )
