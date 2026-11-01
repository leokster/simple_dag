import json
import os

import pytest

from simple_dag import JsonInput, JsonOutput, Multiple, PathInput, storage
from simple_dag.utils import fsspec as sd_fsspec

AZURE_BASE = "abfs://data@account.dfs.core.windows.net"


@pytest.fixture(autouse=True)
def clean_storage(monkeypatch):
    monkeypatch.delenv(storage.ENV_BASE, raising=False)
    monkeypatch.delenv(storage.ENV_STORAGE_OPTIONS, raising=False)
    storage.reset()
    yield
    storage.reset()


def test_resolve_without_base_returns_path_unchanged():
    assert storage.resolve("data/raw/file.csv") == "data/raw/file.csv"


def test_resolve_relative_path_joined_to_base(tmp_path):
    storage.configure(base=str(tmp_path))
    assert storage.resolve("proj/raw/file.csv") == f"{tmp_path}/proj/raw/file.csv"
    assert storage.resolve("./proj/raw/file.csv") == f"{tmp_path}/proj/raw/file.csv"


def test_resolve_absolute_path_and_uri_untouched(tmp_path):
    storage.configure(base=str(tmp_path))
    assert storage.resolve("/elsewhere/file.csv") == "/elsewhere/file.csv"
    assert storage.resolve("s3://bucket/key.csv") == "s3://bucket/key.csv"


def test_configure_reads_base_from_environment(tmp_path, monkeypatch):
    monkeypatch.setenv(storage.ENV_BASE, str(tmp_path) + "/")
    assert storage.get_base() == str(tmp_path)


def test_configure_relative_local_base_becomes_absolute(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    storage.configure(base=".local/data")
    assert storage.get_base() == os.path.join(str(tmp_path), ".local", "data")


def test_relative_to_base_inverts_resolve(tmp_path):
    storage.configure(base=str(tmp_path))
    assert storage.relative_to_base(storage.resolve("a/b.json")) == "a/b.json"
    assert storage.relative_to_base("/other/b.json") == "/other/b.json"


def test_storage_options_azure_base_defaults_to_authenticated():
    storage.configure(base=AZURE_BASE)
    assert storage.storage_options_for("proj/raw/x.json") == {"anon": False}


def test_storage_options_environment_json_merged(monkeypatch):
    monkeypatch.setenv(storage.ENV_STORAGE_OPTIONS, '{"account_name": "account"}')
    storage.configure(base=AZURE_BASE, storage_options={"anon": True})
    assert storage.storage_options_for("x.json") == {
        "account_name": "account",
        "anon": True,
    }


def test_storage_options_local_write_creates_directories(tmp_path):
    storage.configure(base=str(tmp_path))
    assert storage.storage_options_for("x.json", "w") == {"auto_mkdir": True}
    assert storage.storage_options_for("x.json", "r") == {}


def test_storage_options_outside_base_empty(tmp_path):
    storage.configure(base=str(tmp_path))
    assert storage.storage_options_for("/elsewhere/x.json", "w") == {}


def test_json_round_trip_below_base_creates_parent_dirs(tmp_path):
    storage.configure(base=str(tmp_path))
    JsonOutput("proj/curated/result.json").write_data({"total": 3})
    assert (tmp_path / "proj" / "curated" / "result.json").exists()
    assert JsonInput("proj/curated/result.json").get_data() == {"total": 3}


def test_list_files_below_base_returns_relative_paths(tmp_path):
    storage.configure(base=str(tmp_path))
    for name in ("b", "a"):
        JsonOutput(f"proj/raw/{name}.json").write_data({"name": name})
    assert sd_fsspec.list_files("proj/raw/*.json") == [
        "proj/raw/a.json",
        "proj/raw/b.json",
    ]
    assert Multiple(JsonInput("proj/raw/*.json")).get_data() == [
        {"name": "a"},
        {"name": "b"},
    ]


def test_list_files_absolute_pattern_returns_absolute_paths(tmp_path):
    (tmp_path / "x.json").write_text("{}")
    assert sd_fsspec.list_files(f"{tmp_path}/*.json") == [str(tmp_path / "x.json")]


def test_exists_and_remove_below_base(tmp_path):
    storage.configure(base=str(tmp_path))
    assert sd_fsspec.exists("proj/state/tokens.json") is False
    with sd_fsspec.open("proj/state/tokens.json", "w") as handle:
        json.dump({"token": "x"}, handle)
    assert sd_fsspec.exists("proj/state/tokens.json") is True
    sd_fsspec.remove("proj/state/tokens.json")
    sd_fsspec.remove("proj/state/tokens.json")
    assert sd_fsspec.exists("proj/state/tokens.json") is False


def test_path_input_returns_resolved_path(tmp_path):
    storage.configure(base=str(tmp_path))
    assert PathInput("proj/raw").get_data() == f"{tmp_path}/proj/raw"
