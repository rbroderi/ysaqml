# API Reference

The public API is intentionally small so SQLAlchemy users can reason about the
synchronizer just like any other engine helper.

## Primary Entry Points

- `ysaqml.engine.YamlSqliteEngine`: A dataclass-powered context manager that
  creates an in-memory SQLite engine, loads YAML files, and flushes rows back to
  disk on successful exit. The `engine` attribute exposes the underlying
  SQLAlchemy engine so you can issue queries/manipulations directly.
- `ysaqml.engine.create_yaml_engine`: Convenience helper that builds an engine
  configured for the `ysaqml` dialect and ensures the YAML sync plugin is
  activated.
- Constants exported from `ysaqml`: `DEFAULT_NAAY_VERSION`, `NULL_SENTINEL`,
  `BLOB_SENTINEL_BASE85`, `BLOB_SENTINEL_BASE64`, and
  `BLOB_DEFAULT_SENTINEL`.

## mkdocstrings Output

::: ysaqml.engine.YamlSqliteEngine

::: ysaqml.engine.create_yaml_engine
