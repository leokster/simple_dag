"""Health checks and their structured results.

A health check is a callable that receives the data of an input or output and
returns something truthy when the data is fine::

    def has_rows(df):
        return len(df) > 0

A check is reported under its function name and stops the transform when it does
not pass. Function attributes change both::

    has_rows.check_id = "orders-not-empty"
    has_rows.severity = "warning"  # recorded, but the transform goes on
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger("simple_dag")

BLOCKING = "blocking"


@dataclass(frozen=True)
class CheckResult:
    """Outcome of one health check.

    ``status`` is ``"passed"``, ``"failed"`` or ``"error"`` (the check raised).
    Only a result with ``severity == "blocking"`` stops the transform.
    """

    check_id: str
    status: str
    stage: str = "output"
    severity: str = BLOCKING
    version: str = "1"
    metrics: dict = field(default_factory=dict)
    duration_ms: float = 0
    protocol: int = 1


def check_id(check: Callable) -> str:
    """The name a check is reported under: its ``check_id`` or its own name."""
    return (
        getattr(check, "check_id", None)
        or getattr(check, "__name__", None)
        or type(check).__name__
    )


def check_severity(check: Callable) -> str:
    """``"blocking"`` unless the check carries another ``severity``."""
    return getattr(check, "severity", BLOCKING)


def evaluate_check(check: Callable, data: Any, stage: str = "output") -> CheckResult:
    """Run one health check on ``data`` and report the outcome instead of raising.

    A check passes when it returns something truthy and fails otherwise; an Arrow
    scalar counts by its value. A check that raises is logged and reported with
    status ``"error"``. A check may also return a ``CheckResult`` itself, which is
    passed on unchanged.
    """
    name = check_id(check)
    started = time.monotonic()
    try:
        result = check(data)
        if isinstance(result, CheckResult):
            return result
        if hasattr(result, "as_py"):
            # Older pyarrow treats every scalar as truthy, also a false one.
            result = result.as_py()
        status = "passed" if result else "failed"
    except Exception:
        logger.exception("Health check %s raised an exception", name)
        status = "error"
    return CheckResult(
        name,
        status,
        stage,
        check_severity(check),
        duration_ms=(time.monotonic() - started) * 1000,
    )
