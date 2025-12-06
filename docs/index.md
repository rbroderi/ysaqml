# ysaqml

ysaqml keeps SQLAlchemy tables in an in-memory SQLite database while their
contents are sourced from (and persisted back to) YAML files using the
[`naay`](https://github.com/rbroderi/naay) parser/dumper. The goal is to keep
relational workflows, constraints, and transactional semantics while still
reviewing fixtures as plain text in git.

- Repository: https://github.com/rbroderi/ysaqml
- Python package: `ysaqml`
- Documentation stack: MkDocs + Material with mkdocstrings for the API reference

## Why ysaqml?

- **Plain-text fixtures**: every table maps to `<storage_path>/<table>.yaml`, so
  reviewers can diff data directly.
- **Deterministic round-trips**: naay enforces a strict, string-only YAML
  subset, ensuring loads and saves are lossless (including block literals).
- **SQLAlchemy native**: use the `YamlSqliteEngine` context manager or the
  `ysaqml` dialect via `create_yaml_engine` to wire the synchronizer into any
  workflow.
- **Performance aware**: tables live in SQLite for fast queries; load/save
  operations stream through configurable thread pools to parallelize filesystem
  IO.

## What lives here?

This documentation mirrors the project README while offering more room for
installation guides, usage walkthroughs, and an auto-generated API reference.
Use the navigation links to explore each section, or run `uv run mkdocs serve`
locally to view a live preview while editing the docs.
