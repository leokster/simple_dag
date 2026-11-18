import pytest

from simple_dag import (
    ArrowInput,
    ArrowOutput,
    DirectoryOutput,
    JsonInput,
    JsonOutput,
    Multiple,
    PandasDFOutput,
    SparkDFInput,
    schedule,
    transform,
)
from simple_dag.definitions import (
    PROTOCOL_VERSION,
    definition_manifest,
    manifest_digest,
)


def test_manifest_lists_every_output_and_has_a_stable_digest():
    @transform(
        left=ArrowOutput("unused-left", asset_id="left"),
        right=ArrowOutput("unused-right", asset_id="right"),
    )
    def split(left, right):
        pass

    manifest = definition_manifest([split])
    assert manifest["protocol"] == PROTOCOL_VERSION
    outputs = manifest["transforms"][0]["outputs"]
    assert {key: binding["asset_id"] for key, binding in outputs.items()} == {
        "left": "left",
        "right": "right",
    }
    assert manifest_digest(manifest) == manifest_digest(definition_manifest([split]))


def test_asset_paths_and_source_locations():
    @transform(
        source=ArrowInput("/finance/raw/orders"),
        result=ArrowOutput("/analytics/reports/revenue"),
    )
    def report(source, result):
        pass

    definition = definition_manifest([report])["transforms"][0]
    assert definition["inputs"]["source"]["asset_path"] == "/finance/raw/orders"
    assert definition["outputs"]["result"]["asset_id"] is None
    assert definition["source"]["path"].endswith("test_definitions.py")
    assert definition["source"]["line"] > 0


def test_native_definition_metadata():
    @transform(
        table=PandasDFOutput("/project/data.csv"),
        doc=JsonOutput("/project/doc.json"),
        files=DirectoryOutput("/project/files"),
    )
    def example(table, doc, files):
        pass

    bindings = definition_manifest([example])["transforms"][0]["outputs"]
    assert bindings["doc"]["asset_path"] == "/project/doc.json"
    assert bindings["doc"]["kind"] == "document"
    assert bindings["table"]["format"] == "csv"
    assert bindings["files"]["layout"] == "directory"
    assert bindings["table"]["handler"] == (
        "simple_dag.datahandlers.pandas_handler.PandasDFOutput"
    )


def test_binding_has_exactly_these_fields():
    fields = [{"name": "total", "type": "double", "nullable": True}]

    def has_rows(data):
        return True

    @transform(
        parts=Multiple(JsonInput("/sales/parts/*.json", health_checks=[has_rows])),
        frame=SparkDFInput("staging/frame"),
        table=ArrowOutput(
            "/sales/table", schema_fields=fields, health_checks=[has_rows]
        ),
    )
    def step(parts, frame, table):
        pass

    definition = definition_manifest([step])["transforms"][0]
    assert definition["inputs"] == {
        "parts": {
            "asset_id": None,
            "asset_path": "/sales/parts/*.json",
            "path": "/sales/parts/*.json",
            "kind": "document",
            "format": "json",
            "layout": "pattern",
            "handler": "simple_dag.datahandlers.multiple.Multiple",
            "schema_fields": [],
            "required_checks": [],
        },
        "frame": {
            "asset_id": None,
            "asset_path": None,
            "path": "staging/frame",
            "kind": "table",
            "format": "parquet",
            "layout": "dataset",
            "handler": "simple_dag.datahandlers.spark_handler.SparkDFInput",
            "schema_fields": [],
            "required_checks": [],
        },
    }
    assert definition["outputs"] == {
        "table": {
            "asset_id": None,
            "asset_path": "/sales/table",
            "path": "/sales/table",
            "kind": "table",
            "format": "parquet",
            "layout": "file",
            "handler": "simple_dag.datahandlers.arrow_handler.ArrowOutput",
            "schema_fields": fields,
            "required_checks": ["has_rows"],
        }
    }


def test_logical_asset_is_explicit_or_the_absolute_path():
    fields = [{"name": "total", "type": "double", "nullable": True}]

    @transform(
        relative=JsonInput("sales/orders.json"),
        tied=JsonInput("sales/orders.json").with_asset("/sales/orders"),
        by_id=ArrowInput("/staging/orders", asset_id="orders-id"),
        declared=ArrowOutput("revenue.parquet", asset_path="/sales/revenue"),
        table=PandasDFOutput("/sales/table.csv", schema_fields=fields),
    )
    def step(relative, tied, by_id, declared, table):
        pass

    definition = definition_manifest([step])["transforms"][0]
    bindings = definition["inputs"] | definition["outputs"]
    assert {key: binding["asset_path"] for key, binding in bindings.items()} == {
        "relative": None,
        "tied": "/sales/orders",
        "by_id": None,
        "declared": "/sales/revenue",
        "table": "/sales/table.csv",
    }
    assert bindings["by_id"]["asset_id"] == "orders-id"
    assert bindings["declared"]["path"] == "revenue.parquet"
    assert bindings["table"]["schema_fields"] == fields


def test_required_checks_are_the_blocking_checks_by_id():
    def has_rows(data):
        return True

    def is_recent(data):
        return True

    def is_tidy(data):
        return True

    is_recent.check_id = "freshness"
    is_tidy.severity = "warning"

    @transform(out=JsonOutput("out.json", health_checks=[has_rows, is_recent, is_tidy]))
    def step(out):
        pass

    binding = definition_manifest([step])["transforms"][0]["outputs"]["out"]
    assert binding["required_checks"] == ["has_rows", "freshness"]


def test_schedules_and_timezone():
    @schedule(cron="0 6 * * *", timezone="Europe/Zurich")
    @transform(out=JsonOutput("scheduled.json"))
    def scheduled(out):
        pass

    @transform(out=JsonOutput("manual.json"))
    def manual(out):
        pass

    definitions = definition_manifest([scheduled, manual])["transforms"]
    assert [definition["name"] for definition in definitions] == ["manual", "scheduled"]
    assert (definitions[0]["schedules"], definitions[0]["timezone"]) == ([], "UTC")
    assert (definitions[1]["schedules"], definitions[1]["timezone"]) == (
        ["0 6 * * *"],
        "Europe/Zurich",
    )


def test_duplicate_transform_names_are_rejected():
    def build():
        @transform(out=JsonOutput("out.json"))
        def step(out):
            pass

        return step

    with pytest.raises(ValueError, match="Duplicate transform name: step"):
        definition_manifest([build(), build()])
