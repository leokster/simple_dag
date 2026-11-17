from pathlib import Path

import pytest
from pydantic import BaseModel
from simple_dag import transform
from simple_dag.definitions import definition_manifest
from simple_dag.datahandlers.arrow_handler import ArrowOutput
from simple_dag.datahandlers.binary_handler import BinaryInput, BinaryOutput
from simple_dag.datahandlers.directory_handler import DirectoryInput, DirectoryOutput
from simple_dag.datahandlers.filepath_handler import PathInput
from simple_dag.datahandlers.json_handler import JsonInput, JsonOutput
from simple_dag.datahandlers.multiple import Multiple, MultipleIterator
from simple_dag.datahandlers.pandas_handler import PandasDFInput, PandasDFOutput
from simple_dag.datahandlers.spark_handler import SparkDFInput, SparkDFOutput


@pytest.mark.parametrize(
    "handler",
    [
        BinaryInput,
        BinaryOutput,
        DirectoryInput,
        DirectoryOutput,
        PathInput,
        JsonInput,
        JsonOutput,
        PandasDFInput,
        PandasDFOutput,
        SparkDFInput,
        SparkDFOutput,
        ArrowOutput,
    ],
)
def test_binding_keeps_handler_type_and_declaration(handler, tmp_path):
    declaration = handler("/project/example")
    bound = declaration.bind_path(str(tmp_path / "allocated"))
    assert type(bound) is handler
    assert bound.path != declaration.path
    assert declaration.path == "/project/example"
    assert bound is not declaration


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


@pytest.mark.parametrize("multiple", [Multiple, MultipleIterator])
def test_multiple_preserves_validation_options(multiple, tmp_path):
    class Document(BaseModel):
        value: int

    (tmp_path / "a.json").write_text('{"value":1}')
    original = JsonInput("/project/*.json", schema_validation=Document)
    handler = multiple(original).with_asset("/project/documents")
    bound = handler.bind_files([str(tmp_path / "a.json")], str(tmp_path))
    values = list(bound.get_data())
    assert isinstance(values[0], Document)
    assert values[0].value == 1
    assert original.kwargs == {}


def test_binary_checks_are_not_dropped(tmp_path):
    output = BinaryOutput(str(tmp_path / "data.bin"), health_checks=[lambda data: False])
    with pytest.raises(ValueError, match="checks failed"):
        output.write_data(b"blocked")
    assert not Path(output.path).exists()


def test_spark_binding_preserves_reader_and_checks(tmp_path):
    class Reader:
        def parquet(self, path):
            assert path == str(tmp_path)
            return "frame"

    class Spark:
        read = Reader()

    handler = SparkDFInput("/project/data.parquet", health_checks=[lambda frame: frame == "frame"])
    bound = handler.bind_files(
        [str(tmp_path / "a.parquet"), str(tmp_path / "b.parquet")], str(tmp_path)
    )
    assert bound.get_data(Spark()) == "frame"
    assert bound.check_results[0].status == "passed"
