import functools
import json
import sys

import pytest

from simple_dag import JsonInput, JsonOutput, transform
from simple_dag.transforms import Transform


def test_compute_loads_inputs_and_hands_outputs_to_the_function(tmp_path):
    (tmp_path / "numbers.json").write_text("[1, 2, 3]")

    @transform(
        numbers=JsonInput(str(tmp_path / "numbers.json")),
        out=JsonOutput(str(tmp_path / "total.json")),
    )
    def total(numbers, out):
        out.write_data({"total": sum(numbers)})
        return len(numbers)

    assert total.compute(None) == 3
    assert json.loads((tmp_path / "total.json").read_text()) == {"total": 6}


def test_ctx_goes_only_to_functions_that_take_it():
    received = []

    @transform()
    def with_ctx(ctx):
        received.append(ctx)

    @transform()
    def keyword_only(*, ctx):
        received.append(ctx)

    @transform()
    def without_ctx():
        ctx = "a local variable, not an argument"
        received.append(ctx)

    with_ctx.compute("context")
    keyword_only.compute("context")
    without_ctx.compute("context")
    assert received == ["context", "context", "a local variable, not an argument"]


def test_wrapper_that_supplies_ctx_itself_is_left_alone():
    def with_own_ctx(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            return function(*args, ctx="from the wrapper", **kwargs)

        return wrapper

    @transform()
    @with_own_ctx
    def step(ctx):
        return ctx

    assert step.compute("from simple_dag") == "from the wrapper"


@pytest.mark.skipif(sys.version_info < (3, 14), reason="annotations are lazy from 3.14")
def test_ctx_detection_does_not_evaluate_annotations():
    @transform()
    def step(ctx, numbers: OnlyKnownToTheTypeChecker = None):  # noqa: F821
        return ctx

    assert step.compute("context") == "context"


def test_transform_is_named_after_its_function():
    def step():
        pass

    assert Transform(step).__name__ == "step"
    assert transform()(step).__name__ == "step"
