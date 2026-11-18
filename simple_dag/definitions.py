"""Serializable definitions of transforms.

``definition_manifest`` describes transforms with their inputs, outputs, health
checks and schedules as plain data, so a host can register them without importing
project code itself. The same transforms always give the same manifest and
therefore the same ``manifest_digest``.
"""

import hashlib
import inspect
import json

from simple_dag.checks import BLOCKING, check_id, check_severity
from simple_dag.datahandlers.base_handler import implied_asset_path

PROTOCOL_VERSION = 1


def definition_manifest(transforms) -> dict:
    """Describe ``transforms`` as plain data, sorted by transform name."""
    definitions = {}
    for transform in transforms:
        name = transform.__name__
        if name in definitions:
            raise ValueError("Duplicate transform name: " + name)
        definitions[name] = {
            "name": name,
            "source": _source(getattr(transform, "compute_func", transform)),
            "inputs": {key: _binding(val) for key, val in transform.inputs.items()},
            "outputs": {key: _binding(val) for key, val in transform.outputs.items()},
            "schedules": list(transform.cron),
            "timezone": transform.timezone or "UTC",
        }
    return {
        "protocol": PROTOCOL_VERSION,
        "transforms": [definitions[name] for name in sorted(definitions)],
    }


def manifest_digest(manifest) -> str:
    """SHA-256 of a manifest, stable across runs and machines."""
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _binding(handler) -> dict:
    return {
        "asset_id": handler.asset_id,
        "asset_path": handler.asset_path
        or implied_asset_path(handler.path, handler.asset_id),
        "path": handler.path,
        "kind": handler.asset_kind,
        "format": handler.data_format,
        "layout": handler.storage_layout,
        "handler": type(handler).__module__ + "." + type(handler).__qualname__,
        "schema_fields": list(handler.schema_fields),
        "required_checks": [
            check_id(check)
            for check in handler.health_checks
            if check_severity(check) == BLOCKING
        ],
    }


def _source(function) -> dict | None:
    """Where ``function`` is defined: its module as a file path and the line."""
    source_file = inspect.getsourcefile(function)
    if not source_file:
        return None
    is_package = source_file.endswith("__init__.py")
    return {
        "path": function.__module__.replace(".", "/")
        + ("/__init__.py" if is_package else ".py"),
        "line": inspect.getsourcelines(function)[1],
    }
