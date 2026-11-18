import pytest
from pydantic import BaseModel

from simple_dag import (
    ArrowInput,
    ArrowOutput,
    BinaryInput,
    BinaryOutput,
    DirectoryInput,
    DirectoryOutput,
    JsonInput,
    JsonOutput,
    Multiple,
    MultipleIterator,
    PandasDFInput,
    PandasDFOutput,
    PathInput,
    SparkDFInput,
    SparkDFOutput,
)


@pytest.mark.parametrize(
    "handler",
    [
        ArrowInput,
        ArrowOutput,
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
    ],
)
def test_binding_keeps_handler_type_and_declaration(handler, tmp_path):
    declaration = handler("/project/example")
    bound = declaration.bind_path(str(tmp_path / "allocated"))
    assert type(bound) is handler
    assert bound.path != declaration.path
    assert declaration.path == "/project/example"
    assert bound is not declaration


def test_binding_keeps_options_and_starts_without_check_results(tmp_path):
    (tmp_path / "data.csv").write_text("a;b\n1;2\n")
    declaration = PandasDFInput(
        "/project/table.csv", sep=";", health_checks=[lambda frame: len(frame) == 1]
    )
    first = declaration.bind_path(str(tmp_path / "data.csv"))
    assert list(first.get_data().columns) == ["a", "b"]
    assert [result.status for result in first.check_results] == ["passed"]
    assert first.bind_path(first.path).check_results == []
    assert declaration.check_results == []


def test_with_asset_ties_a_copy_to_a_logical_asset():
    fields = [{"name": "total", "type": "double", "nullable": True}]
    declaration = PandasDFOutput("local/revenue.csv")
    tied = declaration.with_asset("/analytics/revenue", schema_fields=fields)
    assert (tied.path, tied.asset_path, tied.schema_fields) == (
        "local/revenue.csv",
        "/analytics/revenue",
        fields,
    )
    assert (declaration.asset_path, declaration.schema_fields) == (None, [])


@pytest.mark.parametrize(
    ("declaration", "file_name"),
    [
        (PandasDFOutput("/project/table.csv"), "data.csv"),
        (JsonOutput("/project/document"), "data.json"),
        (BinaryOutput("/project/archive.zip"), "data.zip"),
        (BinaryOutput("/project/blob"), "data.bin"),
        (ArrowOutput("/project/table"), "data.parquet"),
        (SparkDFOutput("/project/table"), "data.parquet"),
    ],
)
def test_output_path_is_a_data_file_with_the_declared_extension(
    declaration, file_name, tmp_path
):
    assert declaration.output_path(str(tmp_path)) == str(tmp_path / file_name)


def test_output_path_of_a_directory_output_is_the_root(tmp_path):
    declaration = DirectoryOutput("/project/files")
    assert declaration.output_path(str(tmp_path)) == str(tmp_path)


def test_bind_files_single_file_input_takes_exactly_one_file(tmp_path):
    files = [str(tmp_path / "a.json"), str(tmp_path / "b.json")]
    declaration = JsonInput("/project/document.json")
    assert declaration.bind_files(files[:1], str(tmp_path)).path == files[0]
    with pytest.raises(ValueError, match="JsonInput requires one file"):
        declaration.bind_files(files, str(tmp_path))
    with pytest.raises(ValueError, match="JsonInput requires one file"):
        declaration.bind_files([], str(tmp_path))


def test_bind_files_dataset_input_takes_the_directory_of_its_parts(tmp_path):
    parts = [str(tmp_path / "parts" / name) for name in ("0.parquet", "1.parquet")]
    declaration = ArrowInput("/project/table")
    assert declaration.bind_files(parts, str(tmp_path)).path == str(tmp_path / "parts")
    assert declaration.bind_files(parts[:1], str(tmp_path)).path == parts[0]


def test_bind_files_directory_input_takes_the_root(tmp_path):
    declaration = DirectoryInput("/project/files")
    bound = declaration.bind_files([str(tmp_path / "nested" / "file")], str(tmp_path))
    assert bound.path == str(tmp_path)


@pytest.mark.parametrize("multiple", [Multiple, MultipleIterator])
def test_multiple_preserves_validation_options(multiple, tmp_path):
    class Document(BaseModel):
        value: int

    (tmp_path / "a.json").write_text('{"value":1}')
    original = JsonInput("/project/*.json", schema_validation=Document)
    handler = multiple(original).with_asset("/project/documents")
    bound = handler.bind_files([str(tmp_path / "a.json")], str(tmp_path))
    assert bound.path == str(tmp_path / "*.json")
    values = list(bound.get_data())
    assert isinstance(values[0], Document)
    assert values[0].value == 1
    assert original.kwargs == {}


def test_multiple_subclass_can_still_construct_its_inputs(tmp_path):
    class ByFile(Multiple):
        """Written when ``Multiple`` built a new input for every file."""

        def get_data(self):
            files = sorted(str(file) for file in tmp_path.glob("*.csv"))
            return {
                file: self.input_cls(file, *self.args, **self.kwargs).get_data()
                for file in files
            }

    (tmp_path / "a.csv").write_text("a;b\n1;2\n")
    wrapped = PandasDFInput(
        str(tmp_path / "*.csv"), sep=";", health_checks=[lambda frame: len(frame) == 1]
    )
    frames = ByFile(wrapped).get_data()
    assert list(frames[str(tmp_path / "a.csv")].columns) == ["a", "b"]


def test_multiple_describes_the_data_of_the_wrapped_input():
    wrapped = PandasDFInput("exports/*.csv", name="exports", description="All exports")
    handler = Multiple(wrapped.with_asset("/project/exports"))
    assert (handler.asset_kind, handler.data_format, handler.storage_layout) == (
        "table",
        "csv",
        "pattern",
    )
    assert (handler.path, handler.name, handler.description, handler.asset_path) == (
        "exports/*.csv",
        "exports",
        "All exports",
        "/project/exports",
    )


def test_multiple_runs_the_checks_of_the_wrapped_input_per_file(tmp_path):
    for name, content in (("a.json", "[1]"), ("b.json", "[]")):
        (tmp_path / name).write_text(content)
    pattern = str(tmp_path / "*.json")
    handler = Multiple(JsonInput(pattern, health_checks=[lambda data: len(data) > 0]))
    with pytest.raises(ValueError, match=r"Input checks failed for .*b\.json"):
        handler.get_data()


def test_spark_binding_preserves_reader_and_checks(tmp_path):
    class Reader:
        def parquet(self, path):
            assert path == str(tmp_path)
            return "frame"

    class Spark:
        read = Reader()

    handler = SparkDFInput(
        "/project/data.parquet", health_checks=[lambda frame: frame == "frame"]
    )
    bound = handler.bind_files(
        [str(tmp_path / "a.parquet"), str(tmp_path / "b.parquet")], str(tmp_path)
    )
    assert bound.get_data(Spark()) == "frame"
    assert bound.check_results[0].status == "passed"


def test_spark_input_without_a_session_explains_what_is_missing():
    with pytest.raises(ValueError, match="requires a Spark session"):
        SparkDFInput("/project/data.parquet").get_data()
