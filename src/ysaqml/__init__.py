"""Public package interface for ysaqml."""

from .engine import YamlSqliteEngine as YamlSqliteEngine
from .engine import create_yaml_engine as create_yaml_engine
from .sync import BLOB_DEFAULT_SENTINEL as BLOB_DEFAULT_SENTINEL
from .sync import BLOB_SENTINEL_BASE64 as BLOB_SENTINEL_BASE64
from .sync import BLOB_SENTINEL_BASE85 as BLOB_SENTINEL_BASE85
from .sync import DEFAULT_NAAY_VERSION as DEFAULT_NAAY_VERSION
from .sync import NULL_SENTINEL as NULL_SENTINEL

try:
    from beartype.claw import beartype_this_package as _beartype_this_package

    _beartype_this_package()
except ImportError:  # beartype is optional
    pass
