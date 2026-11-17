"""Isolated transform execution, usable by local and remote runners."""

import copy
from dataclasses import dataclass, field


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
    spark: object = field(default=None, repr=False, compare=False)


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
