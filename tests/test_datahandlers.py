import pkgutil
from importlib import import_module

import pandas as pd
import pytest

import simple_dag
from simple_dag import datahandlers, storage
from simple_dag.datahandlers import (
    ArrowInput,
    ArrowOutput,
    BinaryInput,
    BinaryOutput,
    DataHandler,
    DirectoryInput,
    DirectoryOutput,
    JsonInput,
    JsonOutput,
    PandasDFInput,
    PandasDFOutput,
    PathInput,
    SparkDFInput,
    SparkDFOutput,
)

INPUTS = [
    ArrowInput,
    BinaryInput,
    DirectoryInput,
    JsonInput,
    PandasDFInput,
    PathInput,
    SparkDFInput,
]
OUTPUTS = [
    ArrowOutput,
    BinaryOutput,
    DirectoryOutput,
    JsonOutput,
    PandasDFOutput,
    SparkDFOutput,
]
BASE_CLASSES = {"ABCInput", "ABCOutput", "DataHandler"}


def _handlers_defined_in_the_package():
    names = set()
    for module_info in pkgutil.iter_modules(datahandlers.__path__):
        module = import_module(f"{datahandlers.__name__}.{module_info.name}")
        for name, obj in vars(module).items():
            if (
                isinstance(obj, type)
                and issubclass(obj, DataHandler)
                and obj.__module__ == module.__name__
            ):
                names.add(name)
    return names


def test_datahandlers_exports_every_handler():
    assert set(datahandlers.__all__) == _handlers_defined_in_the_package()


def test_top_level_exports_the_same_handlers():
    for name in set(datahandlers.__all__) - BASE_CLASSES:
        assert name in simple_dag.__all__
        assert getattr(simple_dag, name) is getattr(datahandlers, name)


@pytest.mark.parametrize("handler", INPUTS + OUTPUTS)
def test_every_handler_takes_the_common_options(handler):
    def check(data):
        return True

    declared = handler(
        "some/path", name="orders", description="All orders", health_checks=[check]
    )
    assert declared.path == "some/path"
    assert declared.name == "orders"
    assert declared.description == "All orders"
    assert declared.health_checks == [check]
    assert declared.check_results == []


@pytest.mark.parametrize("handler", [*OUTPUTS, ArrowInput, SparkDFInput])
def test_common_options_keep_their_positional_order(handler):
    def check(data):
        return True

    declared = handler("some/path", "orders", "All orders", [check])
    assert (declared.name, declared.description) == ("orders", "All orders")
    assert declared.health_checks == [check]


@pytest.mark.parametrize(
    "handler", [BinaryInput, DirectoryInput, JsonInput, PandasDFInput, PathInput]
)
def test_inputs_with_reader_options_keep_extra_arguments(handler):
    declared = handler("some/path", "positional", name="orders", sep=";")
    assert declared.name == "orders"
    assert declared.args == ("positional",)
    assert declared.kwargs == {"sep": ";"}


def test_json_output_keeps_schema_validation_as_fifth_argument():
    declared = JsonOutput("doc.json", None, None, None, dict)
    assert declared.schema_validation is dict


@pytest.mark.parametrize("handler", INPUTS + OUTPUTS)
def test_handlers_do_not_share_their_default_health_checks(handler):
    first, second = handler("first"), handler("second")
    first.health_checks.append(lambda data: True)
    assert second.health_checks == []


@pytest.mark.parametrize("handler", [*OUTPUTS, ArrowInput])
def test_logical_asset_can_be_declared_with_keywords(handler):
    fields = [{"name": "total", "type": "double", "nullable": True}]
    declared = handler("local/file", asset_path="/project/asset", schema_fields=fields)
    assert declared.asset_path == "/project/asset"
    assert declared.schema_fields == fields
    assert declared.asset_id is None


@pytest.mark.parametrize("handler", [ArrowInput, ArrowOutput, PandasDFOutput])
def test_absolute_path_is_the_logical_asset_unless_bound_by_id(handler, tmp_path):
    declared = handler("/project/table")
    assert declared.asset_path == "/project/table"
    assert declared.bind_path(str(tmp_path / "staged")).asset_path == "/project/table"
    assert handler("relative/table").asset_path is None
    assert handler("/project/table", asset_id="table-id").asset_path is None


def test_handlers_declared_with_a_pathlib_path_keep_working(tmp_path, monkeypatch):
    monkeypatch.delenv(storage.ENV_BASE, raising=False)
    storage.reset()
    plots = DirectoryOutput(tmp_path / "plots")
    assert plots.path == tmp_path / "plots"
    assert plots.asset_path is None
    assert PathInput(tmp_path).get_data() == tmp_path
    assert DirectoryInput(tmp_path).get_data() == tmp_path
    assert SparkDFOutput(tmp_path / "frame").bind_path(tmp_path).path == str(tmp_path)


def test_options_are_kept_when_the_path_is_assigned_later():
    def check(data):
        return True

    declared = BinaryOutput(None, name="later", health_checks=[check])
    assert (declared.path, declared.name, declared.health_checks) == (
        None,
        "later",
        [check],
    )


def test_pandas_round_trip_passes_options_to_pandas(tmp_path):
    path = str(tmp_path / "table.csv")
    frame = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    PandasDFOutput(path).write_data(frame, index=False, sep=";")
    pd.testing.assert_frame_equal(PandasDFInput(path, sep=";").get_data(), frame)


def test_binary_round_trip(tmp_path):
    path = str(tmp_path / "blob.bin")
    BinaryOutput(path).write_data(b"\x00payload")
    assert BinaryInput(path).get_data() == b"\x00payload"


def test_directory_input_returns_an_existing_directory_only(tmp_path):
    assert DirectoryInput(str(tmp_path)).get_data() == str(tmp_path)
    assert DirectoryInput(str(tmp_path / "missing")).get_data() is None


def test_directory_output_is_filled_through_its_path(tmp_path):
    with pytest.raises(NotImplementedError):
        DirectoryOutput(str(tmp_path)).write_data(b"")


def test_arrow_round_trip(tmp_path):
    pa = pytest.importorskip("pyarrow")
    path = str(tmp_path / "table.parquet")
    table = pa.table({"value": [1, 2]})
    ArrowOutput(path).write_data(table)
    assert ArrowInput(path).get_data().equals(table)


def test_arrow_input_reads_a_directory_of_part_files(tmp_path):
    pa = pytest.importorskip("pyarrow")
    for part, values in enumerate([[1, 2], [3]]):
        path = str(tmp_path / f"part-{part}.parquet")
        ArrowOutput(path).write_data(pa.table({"value": values}))
    table = ArrowInput(str(tmp_path)).get_data()
    assert sorted(table.column("value").to_pylist()) == [1, 2, 3]


def test_arrow_output_writes_a_duckdb_relation(tmp_path):
    pytest.importorskip("pyarrow")
    duckdb = pytest.importorskip("duckdb")
    path = str(tmp_path / "table.parquet")
    ArrowOutput(path).write_data(duckdb.sql("select 42 as answer"))
    assert ArrowInput(path).get_data().to_pylist() == [{"answer": 42}]


def test_arrow_output_rejects_anything_but_a_table(tmp_path):
    pytest.importorskip("pyarrow")
    with pytest.raises(TypeError, match="Arrow Table or DuckDB relation"):
        ArrowOutput(str(tmp_path / "table.parquet")).write_data([{"value": 1}])


def test_arrow_checks_run_like_those_of_every_other_handler(tmp_path):
    pa = pytest.importorskip("pyarrow")

    def has_rows(table):
        return table.num_rows > 0

    path = str(tmp_path / "table.parquet")
    output = ArrowOutput(path, health_checks=[has_rows])
    with pytest.raises(ValueError, match=r"Output checks failed .* has_rows"):
        output.write_data(pa.table({"value": []}))
    output.write_data(pa.table({"value": [1]}))
    source = ArrowInput(path, health_checks=[has_rows])
    assert source.get_data().num_rows == 1
    assert [(result.stage, result.status) for result in source.check_results] == [
        ("input", "passed")
    ]
