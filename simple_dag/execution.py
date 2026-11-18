"""Running a transform with bindings supplied by a host.

``execute`` runs a transform against other inputs and outputs than the declared
ones without touching the declaration, so one process can run several attempts
of the same transform side by side.
"""

import copy
from dataclasses import dataclass, field
from typing import Any, Callable

from simple_dag.definitions import PROTOCOL_VERSION


@dataclass(frozen=True)
class ExecutionContext:
    """What a host tells a transform about the run it is part of.

    A transform receives the context by declaring a ``ctx`` argument. ``spark``
    and ``secrets`` are left out of the representation and of comparisons.
    """

    run_id: str
    attempt_id: str
    project_id: str
    branch_id: str
    environment_id: str
    parameters: dict = field(default_factory=dict)
    configuration: dict = field(default_factory=dict)
    partition: dict = field(default_factory=dict)
    spark: object = field(default=None, repr=False, compare=False)
    secrets: dict = field(default_factory=dict, repr=False, compare=False)

    @property
    def env(self) -> dict:
        """Environment variables explicitly supplied by the execution host."""
        return dict(self.configuration.get("env", {}))


def execute(
    transform,
    context: ExecutionContext,
    inputs: dict,
    outputs: dict,
    emit: Callable[[dict], None] | None = None,
) -> Any:
    """Run ``transform`` with ``inputs`` and ``outputs`` instead of the declared ones.

    Both must name exactly the declared inputs and outputs; usually they are the
    declared handlers rebound with ``bind_files`` and ``bind_path``. The
    transform itself is left untouched. ``emit`` receives a ``started`` and a
    ``completed`` event.
    """
    if set(inputs) != set(transform.inputs) or set(outputs) != set(transform.outputs):
        raise ValueError("Bindings must match the complete transform definition")
    isolated = copy.copy(transform)
    isolated.kwargs = dict(transform.kwargs) | inputs | outputs
    if emit:
        emit(_event("started", context))
    result = isolated.compute(context)
    if emit:
        emit(_event("completed", context))
    return result


def _event(kind: str, context: ExecutionContext) -> dict:
    return {
        "protocol": PROTOCOL_VERSION,
        "kind": kind,
        "run_id": context.run_id,
        "attempt_id": context.attempt_id,
    }
