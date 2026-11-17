import pytest
from simple_dag import transform
from simple_dag.platform import (
    ArrowInput,
    ArrowOutput,
    ExecutionContext,
    definition_manifest,
    evaluate_check,
    execute,
    manifest_digest,
)


def test_boolean_and_exception_checks():
    assert evaluate_check(lambda x: True, None).status == "passed"
    assert evaluate_check(lambda x: False, None).status == "failed"
    assert evaluate_check(lambda x: 1 / 0, None).status == "error"


def test_manifest_multiple_outputs_and_isolated_context(tmp_path):
    pa = pytest.importorskip("pyarrow")

    @transform(
        left=ArrowOutput("unused-left", asset_id="left"),
        right=ArrowOutput("unused-right", asset_id="right"),
    )
    def split(left, right, ctx):
        assert ctx.run_id == "run"
        left.write_data(pa.table({"value": [1]}))
        right.write_data(pa.table({"value": [2]}))

    manifest = definition_manifest([split])
    assert len(manifest["transforms"][0]["outputs"]) == 2
    assert manifest_digest(manifest) == manifest_digest(definition_manifest([split]))
    context = ExecutionContext("run", "attempt", "project", "branch", "environment")
    execute(
        split,
        context,
        {},
        {
            "left": ArrowOutput(str(tmp_path / "left.parquet")),
            "right": ArrowOutput(str(tmp_path / "right.parquet")),
        },
    )
    assert split.outputs["left"].path == "unused-left"
    assert ArrowInput(str(tmp_path / "right.parquet")).get_data().to_pylist() == [{"value": 2}]


def test_asset_paths_and_source_locations():
    @transform(
        source=ArrowInput("/finance/raw/orders"), result=ArrowOutput("/analytics/reports/revenue")
    )
    def report(source, result):
        pass

    definition = definition_manifest([report])["transforms"][0]
    assert definition["inputs"]["source"]["asset_path"] == "/finance/raw/orders"
    assert definition["outputs"]["result"]["asset_id"] is None
    assert definition["source"]["path"].endswith("test_platform.py")
    assert definition["source"]["line"] > 0
