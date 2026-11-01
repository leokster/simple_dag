import pytest

from simple_dag.transforms import (
    find_transform_instances_in_module,
    find_transform_instances_in_package,
)
from tests.sample_pipelines.alpha import transforms as alpha


def test_find_in_package_by_name_returns_every_transform_once():
    found = find_transform_instances_in_package("tests.sample_pipelines")
    assert sorted(t.__name__ for t in found) == ["fetch_numbers", "total"]


def test_find_in_package_by_module_object_matches_by_name():
    import tests.sample_pipelines as package

    by_object = find_transform_instances_in_package(package)
    by_name = find_transform_instances_in_package("tests.sample_pipelines")
    assert [id(t) for t in by_object] == [id(t) for t in by_name]


def test_find_in_module_returns_only_that_module():
    found = find_transform_instances_in_module(alpha)
    assert sorted(t.__name__ for t in found) == ["fetch_numbers", "total"]


def test_find_in_package_missing_package_raises():
    with pytest.raises(ModuleNotFoundError):
        find_transform_instances_in_package("tests.sample_pipelines.missing")


def test_schedule_metadata_recorded_on_transforms():
    assert alpha.fetch_numbers.cron == ["0 6 * * *"]
    assert alpha.fetch_numbers.timezone == "Europe/Zurich"
    assert alpha.total.cron == []
    assert alpha.total.on_upstream_success is True
