"""Base-relative paths.

A pipeline usually reads and writes below one root: a folder on a laptop, a bucket or
an Azure container in production. Configure that root once, either with
``configure(base=...)`` or the ``SIMPLE_DAG_BASE`` environment variable, and give the
handlers relative paths such as ``"sales/curated/orders.csv"``. Absolute paths and URIs
are left untouched, and without a configured base relative paths keep resolving against
the working directory as before, so existing pipelines are unaffected.
"""

import json
import os
from dataclasses import dataclass, field

from fsspec.utils import get_protocol

ENV_BASE = "SIMPLE_DAG_BASE"
ENV_STORAGE_OPTIONS = "SIMPLE_DAG_STORAGE_OPTIONS"

AZURE_PROTOCOLS = ("abfs", "abfss", "az")
LOCAL_PROTOCOLS = ("file", "local")
WRITE_MODE_CHARS = ("w", "a", "x")


@dataclass
class StorageConfig:
    base: str | None = None
    storage_options: dict = field(default_factory=dict)


_config: StorageConfig | None = None


def configure(
    base: str | None = None, storage_options: dict | None = None
) -> StorageConfig:
    """Set the base and the fsspec options used for every path below it.

    Explicit arguments win over ``SIMPLE_DAG_BASE`` and
    ``SIMPLE_DAG_STORAGE_OPTIONS`` (a JSON object). Call without arguments to
    (re)load the environment.
    """
    global _config
    options = {}
    env_options = os.environ.get(ENV_STORAGE_OPTIONS)
    if env_options:
        options.update(json.loads(env_options))
    options.update(storage_options or {})
    chosen_base = base if base is not None else os.environ.get(ENV_BASE)
    _config = StorageConfig(
        base=_normalise_base(chosen_base) if chosen_base else None,
        storage_options=options,
    )
    return _config


def reset() -> None:
    """Forget the configuration; the next access reloads the environment."""
    global _config
    _config = None


def get_config() -> StorageConfig:
    if _config is None:
        return configure()
    return _config


def get_base() -> str | None:
    return get_config().base


def is_relative(path: str) -> bool:
    """True for plain relative paths, i.e. neither a URI nor an absolute path."""
    return "://" not in path and not os.path.isabs(path) and not path.startswith("~")


def resolve(path: str) -> str:
    """The path to hand to fsspec: relative paths are joined to the base."""
    base = get_base()
    if base is None or not is_relative(path):
        return path
    return f"{base}/{_strip_leading_dot(path)}"


def is_below_base(path: str) -> bool:
    base = get_base()
    if base is None:
        return False
    resolved = resolve(path)
    return resolved == base or resolved.startswith(base + "/")


def relative_to_base(path: str) -> str:
    """The inverse of ``resolve`` for paths below the base; others pass through."""
    base = get_base()
    if base is not None and path.startswith(base + "/"):
        return path[len(base) + 1 :]
    return path


def storage_options_for(path: str, mode: str = "r") -> dict:
    """fsspec options for one access.

    Paths below the base get the configured options plus sensible defaults for
    the base's protocol: Azure is accessed authenticated
    (``DefaultAzureCredential`` via ``anon=False``), local writes create missing
    parent directories the way object stores do implicitly. Paths outside the
    base get no options, exactly as before.
    """
    if not is_below_base(path):
        return {}
    options = dict(get_config().storage_options)
    protocol = get_protocol(resolve(path))
    if protocol in AZURE_PROTOCOLS:
        options.setdefault("anon", False)
    elif protocol in LOCAL_PROTOCOLS and any(char in mode for char in WRITE_MODE_CHARS):
        options.setdefault("auto_mkdir", True)
    return options


def _normalise_base(base: str) -> str:
    base = base.rstrip("/")
    if is_relative(base):
        return os.path.abspath(os.path.expanduser(base))
    if base.startswith("~"):
        return os.path.expanduser(base)
    return base


def _strip_leading_dot(path: str) -> str:
    while path.startswith("./"):
        path = path[2:]
    return path.lstrip("/")
