import re

import pytest
from dagster import DefaultScheduleStatus, DefaultSensorStatus, materialize

from simple_dag import JsonInput, JsonOutput, schedule, storage, transform
from simple_dag.orchestrators.dagster import (
    AUTOMATION_SENSOR_NAME,
    build_dagster,
    build_definitions,
)

VALID_NAME = re.compile(r"^[A-Za-z0-9_]+$")


@pytest.fixture(autouse=True)
def base(tmp_path, monkeypatch):
    monkeypatch.delenv(storage.ENV_BASE, raising=False)
    storage.configure(base=str(tmp_path))
    yield tmp_path
    storage.reset()


def _chain():
    @schedule(cron="*/5 * * * *", timezone="Europe/Zurich")
    @transform(out=JsonOutput("proj/raw/numbers.json"))
    def fetch(out):
        out.write_data([1, 2, 3])

    @transform(
        numbers=JsonInput("proj/raw/numbers.json"),
        out=JsonOutput("proj/curated/total.json", name="proj_total"),
    )
    def total(numbers, out):
        out.write_data({"total": sum(numbers)})

    return [fetch, total]


def test_build_definitions_relative_paths_give_stable_keys_and_groups():
    repo = build_definitions(_chain()).get_repository_def()
    keys = {key.to_user_string() for key in repo.assets_defs_by_key}
    assert keys == {"proj_raw_numbers_json", "proj_total"}
    groups = {
        key.to_user_string(): assets_def.group_names_by_key[key]
        for key, assets_def in repo.assets_defs_by_key.items()
    }
    assert groups == {"proj_raw_numbers_json": "proj", "proj_total": "proj"}


def test_build_definitions_wires_dependencies_by_path():
    repo = build_definitions(_chain()).get_repository_def()
    total = next(
        assets_def
        for key, assets_def in repo.assets_defs_by_key.items()
        if key.to_user_string() == "proj_total"
    )
    assert {key.to_user_string() for key in total.dependency_keys} == {
        "proj_raw_numbers_json"
    }


def test_build_definitions_schedule_per_cron_running_in_timezone():
    repo = build_definitions(_chain()).get_repository_def()
    schedules = {s.name: s for s in repo.schedule_defs}
    assert list(schedules) == ["proj_raw_numbers_json_schedule"]
    schedule_def = schedules["proj_raw_numbers_json_schedule"]
    assert schedule_def.cron_schedule == "*/5 * * * *"
    assert schedule_def.execution_timezone == "Europe/Zurich"
    assert schedule_def.default_status == DefaultScheduleStatus.RUNNING
    assert schedule_def.job_name == "proj_raw_numbers_json_job"


def test_build_definitions_running_false_leaves_schedules_stopped():
    repo = build_definitions(_chain(), running=False).get_repository_def()
    assert all(
        s.default_status == DefaultScheduleStatus.STOPPED for s in repo.schedule_defs
    )


def test_build_definitions_two_crons_give_two_schedules():
    @schedule(cron="0 6 * * *")
    @schedule(cron="0 18 * * *")
    @transform(out=JsonOutput("proj/raw/twice.json"))
    def twice(out):
        out.write_data({})

    repo = build_definitions([twice]).get_repository_def()
    assert sorted(s.name for s in repo.schedule_defs) == [
        "proj_raw_twice_json_schedule",
        "proj_raw_twice_json_schedule_2",
    ]


def test_build_definitions_no_sensor_without_eager_transforms():
    repo = build_definitions(_chain()).get_repository_def()
    assert repo.sensor_defs == []


def test_build_definitions_sensor_when_eager_transform_present():
    repo = build_definitions("tests.sample_pipelines").get_repository_def()
    sensors = {s.name: s for s in repo.sensor_defs}
    assert list(sensors) == [AUTOMATION_SENSOR_NAME]
    assert sensors[AUTOMATION_SENSOR_NAME].default_status == DefaultSensorStatus.RUNNING


def test_build_definitions_from_package_name_finds_all_assets():
    repo = build_definitions("tests.sample_pipelines").get_repository_def()
    keys = {key.to_user_string() for key in repo.assets_defs_by_key}
    assert keys == {"alpha_raw_numbers_json", "alpha_total"}


def test_build_definitions_uri_output_gets_valid_asset_name():
    @transform(out=JsonOutput("abfs://c@acct.dfs.core.windows.net/p/a-b.json"))
    def remote(out):
        out.write_data({})

    repo = build_definitions([remote]).get_repository_def()
    (key,) = repo.assets_defs_by_key
    assert VALID_NAME.match(key.to_user_string())
    (assets_def,) = repo.assets_defs_by_key.values()
    assert assets_def.group_names_by_key[key] == "default"


def test_build_definitions_duplicate_name_for_two_paths_raises():
    @transform(out=JsonOutput("proj/raw/one.json", name="same"))
    def one(out):
        out.write_data({})

    @transform(out=JsonOutput("proj/raw/two.json", name="same"))
    def two(out):
        out.write_data({})

    with pytest.raises(ValueError, match="Duplicate name"):
        build_definitions([one, two])


def test_build_definitions_transform_without_output_raises():
    @transform(numbers=JsonInput("proj/raw/numbers.json"))
    def sink(numbers):
        return numbers

    with pytest.raises(ValueError, match="No output found"):
        build_definitions([sink])


def test_materialize_chain_writes_below_base(base):
    defs = build_definitions(_chain())
    assets = list(defs.get_repository_def().assets_defs_by_key.values())
    result = materialize(assets)
    assert result.success
    assert (base / "proj" / "curated" / "total.json").read_text().strip() == (
        '{\n  "total": 6\n}'
    )


def test_legacy_build_dagster_still_returns_assets_and_sources():
    fetch, total = _chain()
    asset_fns, static_assets = build_dagster([total])
    assert [fn.key.to_user_string() for fn in asset_fns] == ["proj_total"]
    assert [spec.key.to_user_string() for spec in static_assets] == [
        "proj_raw_numbers_json"
    ]


def _metadata_by_key(repo):
    """Plain metadata values per asset key, whether or not Dagster wrapped them."""
    return {
        key.to_user_string(): {
            name: getattr(value, "value", value)
            for name, value in assets_def.specs_by_key[key].metadata.items()
        }
        for key, assets_def in repo.assets_defs_by_key.items()
    }


def test_build_definitions_metadata_records_the_handler_path():
    metadata = _metadata_by_key(build_definitions(_chain()).get_repository_def())
    assert metadata["proj_total"]["simple_dag/path"] == "proj/curated/total.json"
    assert "dagster/uri" not in metadata["proj_total"]


def test_build_definitions_metadata_resolves_uri_below_remote_base():
    storage.configure(base="abfs://c@acct.dfs.core.windows.net")

    @transform(
        numbers=JsonInput("proj/raw/numbers.json"),
        out=JsonOutput("proj/curated/total.json"),
    )
    def total(numbers, out):
        out.write_data({})

    metadata = _metadata_by_key(build_definitions([total]).get_repository_def())
    assert metadata["proj_curated_total_json"]["dagster/uri"] == (
        "abfs://c@acct.dfs.core.windows.net/proj/curated/total.json"
    )
    assert metadata["proj_raw_numbers_json"] == {
        "simple_dag/path": "proj/raw/numbers.json",
        "dagster/uri": "abfs://c@acct.dfs.core.windows.net/proj/raw/numbers.json",
    }


def test_build_definitions_metadata_keeps_absolute_uri():
    uri = "abfs://c@acct.dfs.core.windows.net/p/a.json"

    @transform(out=JsonOutput(uri))
    def remote(out):
        out.write_data({})

    repo = build_definitions([remote]).get_repository_def()
    (entry,) = _metadata_by_key(repo).values()
    assert entry == {"simple_dag/path": uri, "dagster/uri": uri}
