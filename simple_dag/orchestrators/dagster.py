import logging
import os
import re
import warnings
from datetime import timedelta
from types import ModuleType

import dagster
from dagster import (
    AssetExecutionContext,
    AssetSelection,
    AssetSpec,
    AutomationCondition,
    AutomationConditionSensorDefinition,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    FreshnessPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from simple_dag import storage
from simple_dag.transforms import (
    Transform,
    find_transform_instances_in_folder,
    find_transform_instances_in_module,
    find_transform_instances_in_package,
)

for _warning_name in ("PreviewWarning", "BetaWarning", "SupersessionWarning"):
    _warning = getattr(dagster, _warning_name, None)
    if _warning is not None:
        warnings.filterwarnings("ignore", category=_warning)

logger = logging.getLogger("simple_dag")

# Dagster accepts only [A-Za-z0-9_] in asset, job and schedule names.
_INVALID_NAME_CHARS = re.compile(r"[^A-Za-z0-9_]")

AUTOMATION_SENSOR_NAME = "default_automation_condition_sensor"


def _dataset_name_from_path(path):
    stem = storage.relative_to_base(path).split("//")[-1].replace("*", "X")
    return _INVALID_NAME_CHARS.sub("_", stem)


class _AssetNaming:
    """Asset names, descriptions and groups for one build.

    Explicit ``name=`` on a handler wins over the path-derived name; a name may
    only point at one path. Groups are the first segment of a base-relative
    path, which is the project folder in a ``<project>/<layer>/...`` layout.
    """

    def __init__(self, transforms):
        self._names = {}
        self._descriptions = {}
        for transform in transforms:
            for val in [*transform.inputs.values(), *transform.outputs.values()]:
                if val.description:
                    if val.path in self._descriptions:
                        warnings.warn(
                            f"Overriding description for {val.path} from "
                            f"{self._descriptions[val.path]} to {val.description}"
                        )
                    self._descriptions[val.path] = val.description

                if val.name:
                    for known_path, known_name in self._names.items():
                        if known_name == val.name and known_path != val.path:
                            raise ValueError(
                                f"Duplicate name {val.name} found for {val.path} "
                                f"and {known_path}"
                            )
                    self._names[val.path] = val.name

    def name(self, path):
        return self._names.get(path, _dataset_name_from_path(path))

    def description(self, path):
        return self._descriptions.get(path, "") + "\n\n" + f"Path: {path}"

    def group(self, path):
        if not (storage.is_relative(path) and storage.is_below_base(path)):
            return None
        first_segment = storage.relative_to_base(storage.resolve(path)).split("/")[0]
        group = _INVALID_NAME_CHARS.sub("_", first_segment)
        return group or None


def _single_output(transform: Transform):
    outputs = list(transform.outputs.values())
    if len(outputs) > 1:
        raise ValueError("Only one output per transform is supported")
    if len(outputs) == 0:
        raise ValueError("No output found")
    return outputs[0]


def _generate_static_assets(inputs, outputs, naming: _AssetNaming):
    input_paths = [x.path for x in inputs]
    output_paths = [x.path for x in outputs]

    # get all input_paths which are not in output_paths
    # these are the static assets
    static_assets_paths = list(set(input_paths) - set(output_paths))

    assets = []
    for path in sorted(static_assets_paths):
        assets.append(
            AssetSpec(
                key=naming.name(path),
                description=naming.description(path),
                group_name=naming.group(path),
            )
        )

    return assets


def _generate_dagster_asset(
    transformer: Transform, naming: _AssetNaming, cron_via_automation: bool
):
    inputs = transformer.inputs
    output = _single_output(transformer)
    asset_name = naming.name(output.path)
    logger.debug("Asset %s from transform %s", asset_name, transformer.__name__)

    freshness_policy = None
    automation_condition = None

    if transformer.on_upstream_success:
        automation_condition = AutomationCondition.eager()

    if transformer.cron:
        cron = transformer.cron[0]
        freshness_policy = FreshnessPolicy.cron(
            deadline_cron=cron,
            lower_bound_delta=timedelta(minutes=1),
        )
        if automation_condition is None and cron_via_automation:
            automation_condition = AutomationCondition.on_cron(cron)

    @asset(
        name=asset_name,
        deps=[naming.name(val.path) for val in inputs.values()],
        description=naming.description(output.path),
        group_name=naming.group(output.path),
        freshness_policy=freshness_policy,
        automation_condition=automation_condition,
    )
    def asset_fn(context: AssetExecutionContext) -> None:
        context.log.info(
            "Running transform %s -> %s", transformer.__name__, output.path
        )
        transformer()

    return asset_fn


def _build_assets(all_transforms, naming: _AssetNaming, cron_via_automation: bool):
    asset_fns = []
    inputs = []
    outputs = []
    for transform in all_transforms:
        asset_fns.append(
            _generate_dagster_asset(transform, naming, cron_via_automation)
        )
        inputs += list(transform.inputs.values())
        outputs += list(transform.outputs.values())

    static_assets = _generate_static_assets(inputs, outputs, naming)
    return asset_fns, static_assets


def _transforms_from_source(source):
    if isinstance(source, (list, tuple)):
        return list(source)
    if isinstance(source, ModuleType):
        if hasattr(source, "__path__"):
            return find_transform_instances_in_package(source)
        return find_transform_instances_in_module(source)
    if isinstance(source, str):
        if os.path.isdir(source):
            return find_transform_instances_in_folder(source)
        return find_transform_instances_in_package(source)
    raise TypeError(
        "source must be a list of transforms, a module, a package name or a folder"
    )


def build_definitions(source, *, running: bool = True) -> Definitions:
    """Build a Dagster ``Definitions`` object from transforms.

    Every transform becomes an asset; every ``@schedule(cron=...)`` becomes a
    ``ScheduleDefinition`` that materializes that asset, and
    ``on_upstream_success`` transforms are driven by an automation-condition
    sensor. Schedules and the sensor start in the RUNNING state unless
    ``running`` is False.

    Args:
        source: a list of transforms, an imported module or package, a dotted
            package name, or a folder path.
        running: default status of the generated schedules and sensor.

    Example:
        >>> defs = build_definitions("my_project.pipelines")
    """
    transforms = _transforms_from_source(source)
    naming = _AssetNaming(transforms)
    computed_assets, static_assets = _build_assets(
        transforms, naming, cron_via_automation=False
    )

    schedule_status = (
        DefaultScheduleStatus.RUNNING if running else DefaultScheduleStatus.STOPPED
    )
    schedules = []
    for transform in transforms:
        asset_name = naming.name(_single_output(transform).path)
        for index, cron in enumerate(transform.cron):
            suffix = "" if index == 0 else f"_{index + 1}"
            schedules.append(
                ScheduleDefinition(
                    name=f"{asset_name}_schedule{suffix}",
                    job=define_asset_job(
                        f"{asset_name}_job{suffix}",
                        selection=AssetSelection.assets(asset_name),
                    ),
                    cron_schedule=cron,
                    execution_timezone=transform.timezone,
                    default_status=schedule_status,
                )
            )

    sensors = []
    if any(transform.on_upstream_success for transform in transforms):
        sensors.append(
            AutomationConditionSensorDefinition(
                AUTOMATION_SENSOR_NAME,
                target=AssetSelection.all(),
                default_status=(
                    DefaultSensorStatus.RUNNING
                    if running
                    else DefaultSensorStatus.STOPPED
                ),
            )
        )

    return Definitions(
        assets=[*computed_assets, *static_assets],
        schedules=schedules,
        sensors=sensors,
    )


def build_dagster_from_folder(transforms_folder):
    """
    Builds a dagster pipeline from a folder containing transform files.

    The function is looking for functions which are decorated with
    `@simple_dag.transform` and builds a dagster pipeline from them.

    Args:
        transforms_folder (str): Path to the folder containing the transform files.

    Returns:
        tuple: Tuple of asset functions and static assets.
    """
    all_transforms = find_transform_instances_in_folder(transforms_folder)
    return build_dagster(all_transforms)


def build_dagster(all_transforms):
    """
    Builds a dagster pipeline from a list of transform instances.

    Cron schedules are expressed as ``AutomationCondition.on_cron`` here and need
    an automation-condition sensor to fire; prefer ``build_definitions`` for
    real ``ScheduleDefinition`` objects.

    Args:
        all_transforms (list): List of transform instances.

    Returns:
        tuple: Tuple of asset functions and static assets.
    """
    naming = _AssetNaming(all_transforms)
    return _build_assets(all_transforms, naming, cron_via_automation=True)
