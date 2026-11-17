"""Compatibility imports for simpleDag 1.3.0 clients.

New code imports handlers from datahandlers, and execution/definitions/checks
from their respective modules. No platform-specific behavior lives here.
"""

from .checks import CheckResult, evaluate_check
from .datahandlers.arrow_handler import ArrowInput, ArrowOutput
from .definitions import PROTOCOL_VERSION, definition_manifest, manifest_digest
from .execution import ExecutionContext, execute

__all__ = [
    "CheckResult",
    "evaluate_check",
    "ArrowInput",
    "ArrowOutput",
    "PROTOCOL_VERSION",
    "definition_manifest",
    "manifest_digest",
    "ExecutionContext",
    "execute",
]
