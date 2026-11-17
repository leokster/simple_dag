"""Structured results shared by all data handlers."""

import time
from dataclasses import dataclass, field
from typing import Any, Callable


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
