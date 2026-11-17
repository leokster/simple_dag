"""Serializable definitions for every standard data handler."""

import hashlib
import inspect
import json

PROTOCOL_VERSION = 1


def definition_manifest(transforms):
    names = set()
    definitions = []
    for transform in transforms:
        name = transform.__name__
        if name in names:
            raise ValueError("Duplicate transform name: " + name)
        names.add(name)

        def binding(handler):
            return {
                "asset_id": getattr(handler, "asset_id", None),
                "asset_path": getattr(handler, "asset_path", None)
                or (
                    handler.path
                    if handler.path.startswith("/") and not getattr(handler, "asset_id", None)
                    else None
                ),
                "path": handler.path,
                "kind": handler.asset_kind,
                "format": handler.data_format,
                "layout": handler.storage_layout,
                "handler": type(handler).__module__ + "." + type(handler).__qualname__,
                "schema_fields": getattr(handler, "schema_fields", []),
                "required_checks": [
                    getattr(c, "check_id", c.__name__)
                    for c in getattr(handler, "health_checks", [])
                    if getattr(c, "severity", "blocking") == "blocking"
                ],
            }

        function = getattr(transform, "compute_func", transform)
        module = function.__module__
        source_file = inspect.getsourcefile(function)
        source = None
        if source_file:
            module_path = module.replace(".", "/")
            source = {
                "path": module_path
                + ("/__init__.py" if source_file.endswith("__init__.py") else ".py"),
                "line": inspect.getsourcelines(function)[1],
            }
        definitions.append(
            {
                "name": name,
                "source": source,
                "inputs": {k: binding(v) for k, v in transform.inputs.items()},
                "outputs": {k: binding(v) for k, v in transform.outputs.items()},
                "schedules": list(transform.cron),
                "timezone": transform.timezone or "UTC",
            }
        )
    return {
        "protocol": PROTOCOL_VERSION,
        "transforms": sorted(definitions, key=lambda d: d["name"]),
    }


def manifest_digest(manifest):
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
