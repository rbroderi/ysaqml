# ysaqml

`ysaqml` provides a YAML-backed persistence layer for SQLAlchemy. Tables live in
memory inside SQLite for fast queries, while their contents are loaded from –
and saved back to – `.yaml` files using the blazing-fast [`naay`](https://github.com/rbroderi/naay)
parser/dumper.

## Why?

- Keep relational workflows while still committing plain-text fixtures.
- Edit data in YAML, rely on SQLite for constraints and transactions.
- Round-trip friendly thanks to naay's strict string-only subset.

## Installation

```bash
pip install -e .
```

This pulls in `SQLAlchemy` and `naay`. Tests rely on `pytest` (install with
`pip install -e .[test]`).

## Usage

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

data_dir = Path("./data")

with YamlSqliteEngine(metadata, data_dir) as backend:
		engine = backend.engine
		with engine.begin() as conn:
				conn.execute(users.insert().values(id=1, name="Ada"))
				result = conn.execute(select(users)).mappings().all()
				print(result)
# context manager flushes SQLite rows back into data/users.yaml
```

Each SQLAlchemy table is materialized to `<data_dir>/<table_name>.yaml`. When
entering the context the engine wipes any existing rows in the in-memory
database, rehydrates them from YAML if the file exists, and on successful
exit dumps the SQLite contents back to YAML.

## File format

Every YAML document uses the following schema:

```yaml
_naay_version: "2025.12.03-0"
rows:
	- id: "1"
		name: "Ada"
```

- `_naay_version` is validated but not enforced – it defaults to
	`ysaqml.DEFAULT_NAAY_VERSION` and is kept intact when re-saving.
- `rows` is an ordered list of dictionaries where every value is a string.

`ysaqml` serializes Python `None` values as the sentinel string `__NULL__`. When
loading back, that string becomes `None` again, so avoid storing it as a
literal value.

## Testing

```bash
pytest
```

The tests create temporary directories and exercise the full
load → mutate → dump loop.
