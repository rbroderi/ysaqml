from __future__ import annotations

import naay
import pytest
from sqlalchemy import Boolean, Column, Integer, MetaData, String, Table, select

from ysaqml import DEFAULT_NAAY_VERSION, NULL_SENTINEL, YamlSqliteEngine


@pytest.fixture()
def metadata() -> MetaData:
    return MetaData()


def test_round_trip_loads_and_saves(tmp_path, metadata) -> None:
    users = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(40), nullable=False),
        Column("is_active", Boolean, nullable=False, default=True),
    )

    data_dir = tmp_path / "data"
    data_dir.mkdir()

    payload = {
        "_naay_version": DEFAULT_NAAY_VERSION,
        "rows": [
            {"id": "1", "name": "Ada", "is_active": "true"},
        ],
    }
    (data_dir / "users.yaml").write_text(naay.dumps(payload), encoding="utf-8")

    with YamlSqliteEngine(metadata, data_dir) as backend:
        with backend.engine.begin() as conn:
            existing = conn.execute(select(users)).mappings().all()
            assert len(existing) == 1
            assert existing[0]["id"] == 1
            assert existing[0]["name"] == "Ada"
            assert bool(existing[0]["is_active"]) is True
            conn.execute(users.insert().values(id=2, name="Grace", is_active=False))

    saved = naay.loads((data_dir / "users.yaml").read_text(encoding="utf-8"))
    assert saved["rows"] == [
        {"id": "1", "name": "Ada", "is_active": "true"},
        {"id": "2", "name": "Grace", "is_active": "false"},
    ]


def test_null_round_trip(tmp_path, metadata) -> None:
    notes = Table(
        "notes",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("body", String(120), nullable=True),
    )

    data_dir = tmp_path / "data"

    with YamlSqliteEngine(metadata, data_dir) as backend:
        with backend.engine.begin() as conn:
            conn.execute(notes.insert().values(id=1, body=None))

    payload = naay.loads((data_dir / "notes.yaml").read_text(encoding="utf-8"))
    assert payload["rows"] == [
        {"id": "1", "body": NULL_SENTINEL},
    ]

    with YamlSqliteEngine(metadata, data_dir) as backend:
        with backend.engine.begin() as conn:
            row = conn.execute(select(notes)).mappings().one()
            assert row["body"] is None


def test_commit_flushes_immediately(tmp_path, metadata) -> None:
    users = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(40), nullable=False),
        Column("is_active", Boolean, nullable=False),
    )

    data_dir = tmp_path / "data"

    with YamlSqliteEngine(metadata, data_dir) as backend:
        with backend.engine.begin() as conn:
            conn.execute(users.insert().values(id=1, name="Ada", is_active=True))

        payload = naay.loads((data_dir / "users.yaml").read_text(encoding="utf-8"))
        assert payload["rows"] == [
            {"id": "1", "name": "Ada", "is_active": "true"},
        ]


def test_rollback_restores_last_persisted_state(tmp_path, metadata) -> None:
    users = Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(40), nullable=False),
        Column("is_active", Boolean, nullable=False),
    )

    data_dir = tmp_path / "data"

    with YamlSqliteEngine(metadata, data_dir) as backend:
        with backend.engine.begin() as conn:
            conn.execute(users.insert().values(id=1, name="Ada", is_active=True))

        with pytest.raises(RuntimeError):
            with backend.engine.begin() as conn:
                conn.execute(users.insert().values(id=2, name="Grace", is_active=False))
                raise RuntimeError("boom")

        payload = naay.loads((data_dir / "users.yaml").read_text(encoding="utf-8"))
        assert payload["rows"] == [
            {"id": "1", "name": "Ada", "is_active": "true"},
        ]

        with backend.engine.begin() as conn:
            rows = conn.execute(select(users)).mappings().all()
            assert len(rows) == 1
            assert rows[0]["id"] == 1
