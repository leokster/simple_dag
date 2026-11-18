"""Handlers a project defines itself keep working the way they were written."""

import pytest
from dagster import materialize

from simple_dag import Multiple, storage, transform
from simple_dag.datahandlers import ABCInput, ABCOutput
from simple_dag.definitions import definition_manifest
from simple_dag.orchestrators.dagster import build_definitions


def _read_text(self):
    with open(self.path) as f:
        return f.read()


class SkipsBaseConstructor(ABCInput):
    """How the built-in handlers were written before the base had a constructor."""

    def __init__(self, path, name=None, description=None):
        self.path = path
        self.name = name
        self.description = description

    get_data = _read_text


class CallsBaseConstructorFirst(ABCInput):
    def __init__(self, path, name=None, description=None):
        super().__init__()
        self.path = path
        self.name = name
        self.description = description

    get_data = _read_text


class CallsBaseConstructorLast(ABCInput):
    def __init__(self, path, name=None, description=None):
        self.path = path
        self.name = name
        self.description = description
        super().__init__()

    get_data = _read_text


class FixedPath(ABCInput):
    path = "/project/fixed"

    def get_data(self):
        return self.path


class Marker(ABCInput):
    """Declares a dependency without loading anything."""

    def __init__(self, path):
        self.path = path


class TextOutput(ABCOutput):
    def __init__(self, path, name=None, description=None):
        self.path = path
        self.name = name
        self.description = description

    def write_data(self, data):
        with open(self.path, "w") as f:
            f.write(data)


CUSTOM_INPUTS = [
    SkipsBaseConstructor,
    CallsBaseConstructorFirst,
    CallsBaseConstructorLast,
]


@pytest.fixture(autouse=True)
def no_base(monkeypatch):
    monkeypatch.delenv(storage.ENV_BASE, raising=False)
    storage.reset()
    yield
    storage.reset()


@pytest.mark.parametrize("handler", CUSTOM_INPUTS)
def test_custom_input_keeps_what_its_constructor_set(handler):
    declared = handler("/project/notes.txt", "notes", "Raw notes")
    assert (declared.path, declared.name, declared.description) == (
        "/project/notes.txt",
        "notes",
        "Raw notes",
    )
    assert list(declared.health_checks) == []
    assert list(declared.check_results) == []


@pytest.mark.parametrize("handler", CUSTOM_INPUTS)
def test_custom_input_binds_checks_and_appears_in_the_manifest(handler, tmp_path):
    (tmp_path / "notes.txt").write_text("hello")
    declared = handler("/project/notes.txt")
    bound = declared.bind_files([str(tmp_path / "notes.txt")], str(tmp_path))
    bound.check(bound.get_data(), "input")
    assert (bound.get_data(), bound.check_results) == ("hello", [])
    assert declared.path == "/project/notes.txt"

    @transform(notes=declared)
    def step(notes):
        return notes

    binding = definition_manifest([step])["transforms"][0]["inputs"]["notes"]
    assert binding == {
        "asset_id": None,
        "asset_path": "/project/notes.txt",
        "path": "/project/notes.txt",
        "kind": "file",
        "format": "binary",
        "layout": "file",
        "handler": f"{__name__}.{handler.__name__}",
        "schema_fields": [],
        "required_checks": [],
    }


def test_handler_with_a_class_level_path_needs_no_constructor():
    assert FixedPath().get_data() == "/project/fixed"


def test_input_without_get_data_hands_over_none():
    @transform(dependency=Marker("upstream/asset"))
    def step(dependency):
        return dependency

    assert step.compute(None) is None


def test_output_without_write_data_does_nothing():
    class Placeholder(ABCOutput):
        def __init__(self, path):
            self.path = path

    assert Placeholder("anywhere").write_data("ignored") is None


def test_custom_input_inside_multiple(tmp_path):
    for name in ("a.txt", "b.txt"):
        (tmp_path / name).write_text(name)
    handler = Multiple(SkipsBaseConstructor(str(tmp_path / "*.txt"), "notes"))
    assert handler.name == "notes"
    assert sorted(handler.get_data()) == ["a.txt", "b.txt"]


def test_custom_handlers_through_dagster(tmp_path):
    (tmp_path / "notes.txt").write_text("hello")

    @transform(
        notes=CallsBaseConstructorFirst(str(tmp_path / "notes.txt"), "raw_notes"),
        shout=TextOutput(str(tmp_path / "shout.txt"), "shouted_notes"),
    )
    def shout(notes, shout):
        shout.write_data(notes.upper())

    repo = build_definitions([shout]).get_repository_def()
    assert {key.to_user_string() for key in repo.assets_defs_by_key} == {
        "raw_notes",
        "shouted_notes",
    }
    result = materialize(list(repo.assets_defs_by_key.values()))
    assert result.success
    assert (tmp_path / "shout.txt").read_text() == "HELLO"
