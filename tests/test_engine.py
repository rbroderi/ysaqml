from __future__ import annotations

from pathlib import Path
from types import TracebackType
from typing import Any
from typing import cast

import naay
import pytest
from naay import YamlValue
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import LargeBinary
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import select
from sqlalchemy.engine import Engine

from ysaqml import DEFAULT_NAAY_VERSION
from ysaqml import NULL_SENTINEL
from ysaqml import YamlSqliteEngine
from ysaqml import create_yaml_engine
from ysaqml.sync import BLOB_LINE_WIDTH
from ysaqml.sync import BLOB_SENTINEL


@pytest.fixture
def metadata() -> MetaData:
    return MetaData()


def require_engine(engine: Engine | None) -> Engine:
    if engine is None:  # pragma: no cover - defensive guard
        msg = "YamlSqliteEngine did not initialize an engine"
        raise AssertionError(msg)
    return engine


def load_yaml_dict(path: Path) -> dict[str, Any]:
    payload = naay.loads(path.read_text(encoding="utf-8"))
    if not isinstance(
        payload,
        dict,
    ):  # pragma: no cover - test fixtures guarantee shape
        msg = "Expected naay payload to be a mapping"
        raise TypeError(msg)
    return cast(dict[str, Any], payload)


def load_rows(path: Path) -> list[dict[str, str]]:
    payload = load_yaml_dict(path)
    rows = payload.get("rows", [])
    if not isinstance(rows, list):  # pragma: no cover - test fixtures guarantee shape
        msg = f"Expected rows to be a list, got {type(rows)!r}"
        raise TypeError(msg)
    return cast(list[dict[str, str]], rows)


def test_round_trip_loads_and_saves(tmp_path: Path, metadata: MetaData) -> None:
    users = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(40), nullable=False),
        Column("is_active", Boolean, nullable=False, default=True),
    )

    data_dir = tmp_path / "data"
    data_dir.mkdir()

    payload: YamlValue = {
        "_naay_version": DEFAULT_NAAY_VERSION,
        "rows": [
            {"id": "1", "name": "Ada", "is_active": "true"},
        ],
    }
    (data_dir / "users.yaml").write_text(naay.dumps(payload), encoding="utf-8")

    with (
        YamlSqliteEngine(metadata, data_dir) as backend,
        require_engine(backend.engine).begin() as conn,
    ):
        existing = conn.execute(select(users)).mappings().all()
        assert len(existing) == 1
        assert existing[0]["id"] == 1
        assert existing[0]["name"] == "Ada"
        assert bool(existing[0]["is_active"]) is True
        conn.execute(users.insert().values(id=2, name="Grace", is_active=False))

    saved_rows = load_rows(data_dir / "users.yaml")
    assert saved_rows == [
        {"id": "1", "name": "Ada", "is_active": "true"},
        {"id": "2", "name": "Grace", "is_active": "false"},
    ]


def test_null_round_trip(tmp_path: Path, metadata: MetaData) -> None:
    notes = Table(
        "notes",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("body", String(120), nullable=True),
    )

    data_dir = tmp_path / "data"

    with (
        YamlSqliteEngine(metadata, data_dir) as backend,
        require_engine(backend.engine).begin() as conn,
    ):
        conn.execute(notes.insert().values(id=1, body=None))

    payload_rows = load_rows(data_dir / "notes.yaml")
    assert payload_rows == [
        {"id": "1", "body": NULL_SENTINEL},
    ]

    with (
        YamlSqliteEngine(metadata, data_dir) as backend,
        require_engine(backend.engine).begin() as conn,
    ):
        row = conn.execute(select(notes)).mappings().one()
        assert row["body"] is None


def test_blob_round_trip(tmp_path: Path, metadata: MetaData) -> None:
    files = Table(
        "files",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", LargeBinary, nullable=False),
    )

    data_dir = tmp_path / "data"
    blob = b"\x00\x01binary\xff"

    with (
        YamlSqliteEngine(metadata, data_dir) as backend,
        require_engine(backend.engine).begin() as conn,
    ):
        conn.execute(files.insert().values(id=1, payload=blob))

    rows = load_rows(data_dir / "files.yaml")
    stored_payload = rows[0]["payload"]
    assert stored_payload.startswith(BLOB_SENTINEL)
    assert stored_payload != NULL_SENTINEL
    payload_lines = stored_payload.splitlines()
    assert payload_lines[0] == BLOB_SENTINEL
    assert payload_lines[1:]
    assert all(len(line) <= BLOB_LINE_WIDTH for line in payload_lines[1:])
    file_text = (data_dir / "files.yaml").read_text(encoding="utf-8")
    assert "payload: |" in file_text

    with (
        YamlSqliteEngine(metadata, data_dir) as backend,
        require_engine(backend.engine).begin() as conn,
    ):
        row = conn.execute(select(files)).mappings().one()
        assert row["payload"] == blob


def test_save_persists_empty_collections(tmp_path: Path, metadata: MetaData) -> None:
    _notes = Table(
        "notes",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("body", String(120), nullable=True),
    )

    data_dir = tmp_path / "data"

    with YamlSqliteEngine(metadata, data_dir):
        # No rows are inserted; we only invoke the save path.
        pass

    payload = load_yaml_dict(data_dir / "notes.yaml")
    assert payload["_naay_version"] == DEFAULT_NAAY_VERSION
    assert load_rows(data_dir / "notes.yaml") == []


def test_create_yaml_engine_helper(tmp_path: Path, metadata: MetaData) -> None:
    users = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
    )

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    payload: YamlValue = {
        "_naay_version": DEFAULT_NAAY_VERSION,
        "rows": [
            {"id": "1", "name": "Ada"},
        ],
    }
    (data_dir / "users.yaml").write_text(naay.dumps(payload), encoding="utf-8")

    engine = create_yaml_engine(metadata, data_dir)
    try:
        with engine.begin() as conn:
            rows = conn.execute(select(users)).mappings().all()
            assert rows == [{"id": 1, "name": "Ada"}]
            conn.execute(users.insert().values(id=2, name="Grace"))
    finally:
        engine.dispose()

    flushed_rows = load_rows(data_dir / "users.yaml")
    assert flushed_rows == [
        {"id": "1", "name": "Ada"},
        {"id": "2", "name": "Grace"},
    ]


def test_save_falls_back_when_executor_unavailable(
    tmp_path: Path,
    metadata: MetaData,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    users = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
    )

    class ExplodingExecutor:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def __enter__(self) -> ExplodingExecutor:
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> bool:
            return False

        def submit(self, *args: Any, **kwargs: Any) -> None:
            msg = "cannot schedule new futures after interpreter shutdown"
            raise RuntimeError(msg)

    monkeypatch.setattr("ysaqml.sync.ThreadPoolExecutor", ExplodingExecutor)
    data_dir = tmp_path / "data"

    with (
        YamlSqliteEngine(metadata, data_dir) as backend,
        require_engine(backend.engine).begin() as conn,
    ):
        conn.execute(users.insert().values(id=1, name="Ada"))

    rows = load_rows(data_dir / "users.yaml")
    assert rows == [{"id": "1", "name": "Ada"}]
