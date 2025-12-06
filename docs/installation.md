# Installation

## From PyPI

```bash
pip install ysaqml
```

The published wheel pulls in SQLAlchemy and naay. A Rust compiler is **not**
required because naay ships prebuilt artifacts; wheels fall back to the pure
Python implementation automatically when native extensions are unavailable.

## From Source

Clone the repository and install the package in editable mode. The examples
below rely on [uv](https://github.com/astral-sh/uv), but any virtual environment
manager works.

```bash
git clone https://github.com/rbroderi/ysaqml.git
cd ysaqml
uv pip install -e .[dev]
```

Editable installs give you the SQLAlchemy dialect entry points, the test suite,
and optional extras such as `beartype` (enabled via `pip install .[beartype]`).

## Documentation Tooling

The docs are built with MkDocs + Material plus mkdocstrings. Install the
required packages and run the built-in commands:

```bash
uv pip install mkdocs mkdocs-material mkdocstrings[python]
uv run mkdocs serve
```

The live server watches `docs/` and `src/ysaqml/` so edits to code or markdown
refresh the API reference automatically.

## Testing

All code paths are covered by pytest and mypy. Use uv (or your preferred
runner) to execute the checks:

```bash
uv run pytest
uv run mypy src tests
uv run ruff check
```

These commands match the CI workflow and keep the documentation examples
truthful.
