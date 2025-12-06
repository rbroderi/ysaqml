from __future__ import annotations

import base64
import time
from collections.abc import Mapping
from collections.abc import MutableMapping
from collections.abc import Sequence
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import Literal
from typing import Self
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
from ysaqml.sync import BLOB_SENTINEL_BASE64
from ysaqml.sync import BLOB_SENTINEL_BASE85
from ysaqml.sync import YamlSynchronizer


@pytest.fixture
def metadata() -> MetaData:
    return MetaData()


def require_engine(engine: Engine | None) -> Engine:
    if engine is None:  # pragma: no cover - defensive guard
        msg = "YamlSqliteEngine did not initialize an engine"
        raise AssertionError(msg)
    return engine


class ExplodingExecutor:
    """Executor stub that simulates interpreter shutdown."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        return False

    def submit(self, *args: Any, **kwargs: Any) -> None:
        msg = "cannot schedule new futures after interpreter shutdown"
        raise RuntimeError(msg)


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


def build_benchmark_tables(
    metadata: MetaData,
    *,
    prefix: str,
    count: int = 8,
) -> list[Table]:
    tables: list[Table] = []
    for index in range(count):
        tables.append(
            Table(
                f"{prefix}_{index}",
                metadata,
                Column("id", Integer, primary_key=True),
                Column("payload", LargeBinary, nullable=False),
            ),
        )
    return tables


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
    assert stored_payload.startswith(BLOB_SENTINEL_BASE85)
    assert stored_payload != NULL_SENTINEL
    payload_lines = stored_payload.splitlines()
    assert payload_lines[0] == BLOB_SENTINEL_BASE85
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


def test_blob_round_trip_base64_save(tmp_path: Path, metadata: MetaData) -> None:
    files = Table(
        "files",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", LargeBinary, nullable=False),
    )

    data_dir = tmp_path / "data"
    blob = b"\x10\x20base64\x7f"

    with (
        YamlSqliteEngine(
            metadata,
            data_dir,
            blob_encoding=BLOB_SENTINEL_BASE64,
        ) as backend,
        require_engine(backend.engine).begin() as conn,
    ):
        conn.execute(files.insert().values(id=1, payload=blob))

    rows = load_rows(data_dir / "files.yaml")
    stored_payload = rows[0]["payload"]
    payload_lines = stored_payload.splitlines()
    assert payload_lines[0] == BLOB_SENTINEL_BASE64
    encoded = "".join(payload_lines[1:])
    assert encoded == base64.b64encode(blob).decode("ascii")


def test_loads_legacy_base64_blob(tmp_path: Path, metadata: MetaData) -> None:
    files = Table(
        "files",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", LargeBinary, nullable=False),
    )

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    blob = b"\x00\x01legacy\xff"
    encoded = base64.b64encode(blob).decode("ascii")
    payload: YamlValue = {
        "_naay_version": DEFAULT_NAAY_VERSION,
        "rows": [
            {
                "id": "1",
                "payload": f"{BLOB_SENTINEL_BASE64}\n{encoded}",
            },
        ],
    }
    (data_dir / "files.yaml").write_text(naay.dumps(payload), encoding="utf-8")

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


def test_create_yaml_engine_respects_blob_encoding(
    tmp_path: Path,
    metadata: MetaData,
) -> None:
    files = Table(
        "files",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("payload", LargeBinary, nullable=False),
    )

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    blob = b"create-engine"

    engine = create_yaml_engine(
        metadata,
        data_dir,
        blob_encoding=BLOB_SENTINEL_BASE64,
    )
    try:
        with engine.begin() as conn:
            conn.execute(files.insert().values(id=1, payload=blob))
    finally:
        engine.dispose()

    rows = load_rows(data_dir / "files.yaml")
    stored_payload = rows[0]["payload"]
    assert stored_payload.splitlines()[0] == BLOB_SENTINEL_BASE64


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
    monkeypatch.setattr("ysaqml.sync.ThreadPoolExecutor", ExplodingExecutor)
    data_dir = tmp_path / "data"

    with (
        YamlSqliteEngine(metadata, data_dir) as backend,
        require_engine(backend.engine).begin() as conn,
    ):
        conn.execute(users.insert().values(id=1, name="Ada"))

    rows = load_rows(data_dir / "users.yaml")
    assert rows == [{"id": "1", "name": "Ada"}]


def test_load_falls_back_when_executor_unavailable(
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

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    payload: YamlValue = {
        "_naay_version": DEFAULT_NAAY_VERSION,
        "rows": [
            {"id": "1", "name": "Ada"},
        ],
    }
    (data_dir / "users.yaml").write_text(naay.dumps(payload), encoding="utf-8")

    monkeypatch.setattr("ysaqml.sync.ThreadPoolExecutor", ExplodingExecutor)

    with (
        YamlSqliteEngine(metadata, data_dir) as backend,
        require_engine(backend.engine).begin() as conn,
    ):
        rows = conn.execute(select(users)).mappings().all()
        assert rows == [{"id": 1, "name": "Ada"}]


def test_save_threadpool_benchmark(
    tmp_path: Path,
    metadata: MetaData,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tables = build_benchmark_tables(metadata, prefix="save_bench", count=8)

    original_write_rows = YamlSynchronizer._write_rows

    def slow_write(
        self: YamlSynchronizer,
        table: Table,
        rows: Sequence[Mapping[str, str]],
    ) -> None:
        time.sleep(0.05)
        original_write_rows(self, table, rows)

    monkeypatch.setattr(YamlSynchronizer, "_write_rows", slow_write)

    def measure(duration_dir: Path, workers: int) -> float:
        backend = YamlSqliteEngine(
            metadata,
            duration_dir,
            write_workers=workers,
        )
        engine = require_engine(backend.engine)
        backend.metadata.create_all(engine)
        with engine.begin() as conn:
            for idx, table in enumerate(tables):
                conn.execute(
                    table.insert().values(id=idx, payload=b"x" * 32),
                )
        start = time.perf_counter()
        backend.save()
        elapsed = time.perf_counter() - start
        engine.dispose()
        return elapsed

    serial = measure(tmp_path / "save-serial", workers=1)
    parallel = measure(tmp_path / "save-parallel", workers=4)
    assert serial >= parallel * 1.5


def test_load_threadpool_benchmark(
    tmp_path: Path,
    metadata: MetaData,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tables = build_benchmark_tables(metadata, prefix="load_bench", count=8)
    data_dir = tmp_path / "load-data"

    seed_backend = YamlSqliteEngine(metadata, data_dir)
    seed_engine = require_engine(seed_backend.engine)
    seed_backend.metadata.create_all(seed_engine)
    with seed_engine.begin() as conn:
        for idx, table in enumerate(tables):
            conn.execute(
                table.insert().values(id=idx, payload=b"y" * 16),
            )
    seed_backend.save()
    seed_engine.dispose()

    original_read_rows = YamlSynchronizer._read_rows

    def slow_read(
        self: YamlSynchronizer,
        table: Table,
    ) -> Sequence[MutableMapping[str, Any]]:
        time.sleep(0.05)
        return original_read_rows(self, table)

    monkeypatch.setattr(YamlSynchronizer, "_read_rows", slow_read)

    def measure(workers: int) -> float:
        backend = YamlSqliteEngine(
            metadata,
            data_dir,
            write_workers=workers,
        )
        engine = require_engine(backend.engine)
        backend.metadata.create_all(engine)
        start = time.perf_counter()
        backend.load()
        elapsed = time.perf_counter() - start
        engine.dispose()
        return elapsed

    serial = measure(workers=1)
    parallel = measure(workers=4)
    assert serial >= parallel * 1.5
