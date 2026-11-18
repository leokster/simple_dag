import json

import pytest

from simple_dag import (
    ArrowInput,
    ArrowOutput,
    JsonInput,
    JsonOutput,
    SparkDFInput,
    transform,
)
from simple_dag.execution import ExecutionContext, execute

CONTEXT = ExecutionContext("run", "attempt", "project", "branch", "environment")


def _copy_transform():
    @transform(
        source=JsonInput("declared/in.json"), out=JsonOutput("declared/out.json")
    )
    def copy(source, out):
        out.write_data(source)

    return copy


def test_execute_multiple_outputs_and_isolated_context(tmp_path):
    pa = pytest.importorskip("pyarrow")

    @transform(
        left=ArrowOutput("unused-left", asset_id="left"),
        right=ArrowOutput("unused-right", asset_id="right"),
    )
    def split(left, right, ctx):
        assert ctx.run_id == "run"
        left.write_data(pa.table({"value": [1]}))
        right.write_data(pa.table({"value": [2]}))

    execute(
        split,
        CONTEXT,
        {},
        {
            "left": ArrowOutput(str(tmp_path / "left.parquet")),
            "right": ArrowOutput(str(tmp_path / "right.parquet")),
        },
    )
    assert split.outputs["left"].path == "unused-left"
    assert ArrowInput(str(tmp_path / "right.parquet")).get_data().to_pylist() == [
        {"value": 2}
    ]


def test_execute_runs_rebound_handlers_and_leaves_the_declaration(tmp_path):
    (tmp_path / "data.json").write_text("[1, 2, 3]")
    copy = _copy_transform()
    source = copy.inputs["source"].bind_files(
        [str(tmp_path / "data.json")], str(tmp_path)
    )
    out = copy.outputs["out"].bind_path(
        copy.outputs["out"].output_path(str(tmp_path / "staged"))
    )
    (tmp_path / "staged").mkdir()
    execute(copy, CONTEXT, {"source": source}, {"out": out})
    assert json.loads((tmp_path / "staged" / "data.json").read_text()) == [1, 2, 3]
    assert copy.inputs["source"].path == "declared/in.json"
    assert copy.outputs["out"].path == "declared/out.json"


def test_spark_input_reads_through_the_session_of_the_context():
    class Reader:
        def parquet(self, path):
            return f"frame of {path}"

    class Session:
        read = Reader()

    @transform(frame=SparkDFInput("/project/frame"))
    def step(frame):
        return frame

    context = ExecutionContext(
        "run", "attempt", "project", "branch", "environment", spark=Session()
    )
    bindings = {"frame": step.inputs["frame"].bind_path("/staged/frame")}
    assert execute(step, context, bindings, {}) == "frame of /staged/frame"


def test_execute_requires_a_binding_for_every_input_and_output(tmp_path):
    copy = _copy_transform()
    out = JsonOutput(str(tmp_path / "out.json"))
    with pytest.raises(ValueError, match="Bindings must match"):
        execute(copy, CONTEXT, {}, {"out": out})


def test_execute_emits_started_and_completed_events():
    @transform()
    def nothing():
        return "result"

    events = []
    assert execute(nothing, CONTEXT, {}, {}, emit=events.append) == "result"
    assert events == [
        {"protocol": 1, "kind": kind, "run_id": "run", "attempt_id": "attempt"}
        for kind in ("started", "completed")
    ]


def test_execution_context_exposes_host_environment_and_private_secrets():
    received = {}

    @transform()
    def contextual(ctx):
        received.update(
            env=ctx.env, secret=ctx.secrets["api_key"], project=ctx.project_id
        )

    context = ExecutionContext(
        "run",
        "attempt",
        "project",
        "main",
        "development",
        configuration={"env": {"REGION": "CH"}},
        secrets={"api_key": "private-test-value"},
    )
    execute(contextual, context, {}, {})
    assert received == {
        "env": {"REGION": "CH"},
        "secret": "private-test-value",
        "project": "project",
    }
    assert "private-test-value" not in repr(context)
    context.env["REGION"] = "changed"
    assert context.env["REGION"] == "CH"
