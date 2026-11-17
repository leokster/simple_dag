"""Optional platform execution contracts; no dependency on Bedrock or an orchestrator.

Existing decorators and path-based execution remain unchanged. Platform execution
uses copied bindings, so concurrent attempts never mutate transform definitions.
"""

import copy
import hashlib
import inspect
import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from simple_dag.datahandlers.base_handler import ABCInput, ABCOutput

PROTOCOL_VERSION = 1


@dataclass(frozen=True)
class ExecutionContext:
    run_id: str
    attempt_id: str
    project_id: str
    branch_id: str
    environment_id: str
    parameters: dict = field(default_factory=dict)
    configuration: dict = field(default_factory=dict)
    partition: dict = field(default_factory=dict)


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    status: str
    stage: str = "output"
    severity: str = "blocking"
    version: str = "1"
    metrics: dict = field(default_factory=dict)
    duration_ms: float = 0
    protocol: int = 1


def evaluate_check(check: Callable, data: Any, stage="output") -> CheckResult:
    started = time.monotonic()
    name = getattr(check, "check_id", check.__name__)
    severity = getattr(check, "severity", "blocking")
    try:
        result = check(data)
        if isinstance(result, CheckResult):
            return result
        status = "passed" if result is True else "failed"
    except Exception:
        status = "error"
    return CheckResult(
        name, status, stage, severity, duration_ms=(time.monotonic() - started) * 1000
    )


class ArrowInput(ABCInput):
    def __init__(
        self,
        path: str,
        *,
        asset_id=None,
        asset_path=None,
        schema_fields=None,
        health_checks=None,
        name=None,
    ):
        self.path, self.asset_id, self.name = path, asset_id, name
        self.asset_path = asset_path or (
            path if path.startswith("/") and asset_id is None else None
        )
        self.schema_fields, self.health_checks = schema_fields or [], health_checks or []

    def get_data(self):
        import pyarrow.parquet as pq

        table = pq.read_table(self.path)
        self.check_results = [evaluate_check(c, table, "input") for c in self.health_checks]
        if any(c.status != "passed" and c.severity == "blocking" for c in self.check_results):
            raise ValueError("Input checks failed")
        return table


class ArrowOutput(ABCOutput):
    def __init__(
        self,
        path: str,
        *,
        asset_id=None,
        asset_path=None,
        schema_fields=None,
        health_checks=None,
        name=None,
    ):
        self.path, self.asset_id, self.name = path, asset_id, name
        self.asset_path = asset_path or (
            path if path.startswith("/") and asset_id is None else None
        )
        self.schema_fields, self.health_checks = schema_fields or [], health_checks or []
        self.check_results = []

    def write_data(self, data):
        import pyarrow as pa
        import pyarrow.parquet as pq

        if hasattr(data, "fetch_arrow_table"):
            data = data.fetch_arrow_table()
        if not isinstance(data, pa.Table):
            raise TypeError("ArrowOutput requires an Arrow Table or DuckDB relation")
        self.check_results = [evaluate_check(c, data) for c in self.health_checks]
        if any(c.status != "passed" and c.severity == "blocking" for c in self.check_results):
            raise ValueError("Output checks failed")
        pq.write_table(data, self.path)


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
                "asset_path": getattr(handler, "asset_path", None),
                "path": handler.path,
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


def execute(transform, context: ExecutionContext, inputs: dict, outputs: dict, emit=None):
    if set(inputs) != set(transform.inputs) or set(outputs) != set(transform.outputs):
        raise ValueError("Bindings must match the complete transform definition")
    isolated = copy.copy(transform)
    isolated.kwargs = dict(transform.kwargs) | inputs | outputs
    if emit:
        emit(
            {
                "protocol": 1,
                "kind": "started",
                "run_id": context.run_id,
                "attempt_id": context.attempt_id,
            }
        )
    result = isolated.compute(context)
    if emit:
        emit(
            {
                "protocol": 1,
                "kind": "completed",
                "run_id": context.run_id,
                "attempt_id": context.attempt_id,
            }
        )
    return result
