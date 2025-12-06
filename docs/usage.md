# Usage

This page highlights the most common patterns when working with ysaqml. Every
example assumes you already defined SQLAlchemy table metadata (columns, types,
constraints) and a directory that stores the YAML fixtures.

## Context-Managed Engine

The highest-level entry point is `YamlSqliteEngine`, which wraps an in-memory
SQLite engine plus the YAML synchronizer in a context manager. When entering the
context it wipes the SQLite tables, rehydrates them from YAML, and upon a clean
exit flushes the rows back to disk.

```python
from pathlib import Path
from sqlalchemy import Column, Integer, MetaData, String, Table, select

from ysaqml import YamlSqliteEngine

metadata = MetaData()
users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, nullable=False),
)

storage = Path("./data")

with YamlSqliteEngine(metadata, storage) as backend:
    engine = backend.engine
    with engine.begin() as conn:
        conn.execute(users.insert().values(id=1, name="Ada"))
        print(conn.execute(select(users)).mappings().all())
# context exit writes ./data/users.yaml
```

### Configuring Blob Encoding & Workers

`YamlSqliteEngine` forwards keyword arguments to the underlying synchronizer.
The most common tweaks are:

- `write_workers`: controls how many threads parallelize YAML IO. Defaults to
  `min(32, cpu_count * 4)`.
- `blob_encoding`: choose between the default ASCII85 sentinel or the legacy
  Base64 sentinel when serializing `LargeBinary`/`BLOB` columns.
- `null_token` and `naay_version`: override the sentinel and version header
  stored in each YAML document.

```python
with YamlSqliteEngine(
    metadata,
    storage,
    write_workers=8,
    blob_encoding=ysaqml.BLOB_SENTINEL_BASE64,
) as backend:
    ...
```

## Dialect Helper

If you prefer to manage SQLAlchemy engines yourself, use
`ysaqml.create_yaml_engine`. It wires up the custom `ysaqml` dialect, registers
the sync plugin, and configures SQLite with sensible defaults (in-memory DB,
`StaticPool`, `check_same_thread=False`).

```python
from contextlib import closing

engine = create_yaml_engine(metadata, storage)
with closing(engine):
    with engine.begin() as conn:
        conn.execute(users.insert().values(id=2, name="Grace"))
# remember to dispose() or use closing()/contextlib
```

Whenever you dispose the engine, the plugin automatically saves every table to
its YAML companion. If you keep the engine alive across tests or processes,
call `engine.dispose()` explicitly to flush the data.

## File Layout

Each table maps to `<storage_path>/<table_name>.yaml` with the following schema:

```yaml
_naay_version: "1.0"
rows:
  - id: "1"
    name: "Ada"
```

- Values are always strings because naay enforces a strict subset of YAML.
- `None`/`NULL` is encoded as `ysaqml.NULL_SENTINEL` and decoded back when
  loading.
- Binary columns serialize as ASCII85 (or Base64 if configured) using
  block-literal formatting so large payloads stay readable in git diffs.

## Concurrency & Performance

Loads and saves stream through a thread pool. Reads gather every table’s YAML
payload in parallel before inserting into SQLite, while saves encode SQLite
rows and write them out concurrently. Use `write_workers=1` to disable threading
if you need deterministic sequencing, or raise it if IO is the bottleneck.

Benchmarks in `tests/test_engine.py::test_*_threadpool_benchmark` validate that
threaded IO is substantially faster than serial runs by simulating slow
filesystem access.
