import os

from dagster import (
    asset,
    SourceAsset,
    FreshnessPolicy,
    AutoMaterializePolicy,
    ExperimentalWarning,
)

from simple_dag.datahandlers.base_handler import ABCInput, ABCOutput
from simple_dag.transforms import find_transform_instances_in_folder, Transform

import logging
import warnings

warnings.filterwarnings("ignore", category=ExperimentalWarning)


PATH_TO_NICE_NAME = {}
PATH_TO_DESCRIPTION = {}


def _dataset_name_from_path(path):
    return (
        path.split("//")[-1]
        .replace("/", "_")
        .replace("*", "X")
        .replace(".", "_")
        # .replace("-", "_")
    )


def _get_asset_description(path):
    return PATH_TO_DESCRIPTION.get(path, "") + "\n\n" + f"Path: {path}"


def _get_asset_name(path):
    return PATH_TO_NICE_NAME.get(path, _dataset_name_from_path(path))


def _generate_static_assets(inputs, outputs):
    input_paths = [x.path for x in inputs]
    output_paths = [x.path for x in outputs]

    # get all input_paths which are not in output_paths
    # these are the static assets
    static_assets_paths = list(set(input_paths) - set(output_paths))

    assets = []
    for name in static_assets_paths:
        assets.append(
            SourceAsset(
                key=_get_asset_name(name),
                description=_get_asset_description(name),
            )
        )

    return assets


def _generate_dagster_asset(transformer: Transform):
    inputs = {
        key: val for key, val in transformer.kwargs.items() if isinstance(val, ABCInput)
    }
    outputs = {
        key: val
        for key, val in transformer.kwargs.items()
        if isinstance(val, ABCOutput)
    }

    if len(outputs) > 1:
        raise ValueError("Only one output per transform is supported")

    if len(outputs) == 0:
        raise ValueError("No output found")

    output = list(outputs.values())[0]

    print("Asset: ", _get_asset_name(output.path))

    freshness_policy = None
    auto_materialize_policy = None

    if transformer.on_upstream_success:
        auto_materialize_policy = AutoMaterializePolicy.eager()

    if transformer.cron:
        freshness_policy = FreshnessPolicy(
            maximum_lag_minutes=1,
            cron_schedule=transformer.cron[0],
        )
        if auto_materialize_policy is None:
            auto_materialize_policy = AutoMaterializePolicy.lazy()

    @asset(
        name=_get_asset_name(output.path),
        non_argument_deps={_get_asset_name(val.path) for _, val in inputs.items()},
        description=_get_asset_description(output.path),
        freshness_policy=freshness_policy,
        auto_materialize_policy=auto_materialize_policy,
    )
    def asset_fn(**kwargs) -> None:
        transformer()

    return asset_fn


def build_dagster_from_folder(transforms_folder):
    """
    Builds a dagster pipeline from a folder containing transform files.

    The function is looking for functions which are decorated with `@simple_dag.transform`
    and builds a dagster pipeline from them.

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

    Args:
        all_transforms (list): List of transform instances.

    Returns:
        tuple: Tuple of asset functions and static assets.
    """

    asset_fns = []
    inputs = []
    outputs = []
    for transform in all_transforms:
        for _, val in transform.kwargs.items():
            if isinstance(val, ABCInput) or isinstance(val, ABCOutput):
                if val.description:
                    if val.path in PATH_TO_DESCRIPTION.keys():
                        Warning(
                            "Overriding description for {val.path} from {PATH_TO_DESCRIPTION[val.path]} to {val.description}"
                        )
                    PATH_TO_DESCRIPTION[val.path] = val.description

                if val.name:
                    if val.name in PATH_TO_NICE_NAME.values():
                        potential_collision = [
                            key
                            for key, value in PATH_TO_NICE_NAME.items()
                            if value == val.name
                        ]
                        if val.path != potential_collision[0]:
                            raise ValueError(
                                f"Duplicate name {val.name} found for {val.path} and {potential_collision[0]}"
                            )
                    PATH_TO_NICE_NAME[val.path] = val.name

    for transform in all_transforms:
        asset_fn = _generate_dagster_asset(transform)
        asset_fns.append(asset_fn)
        inputs += [
            val for _, val in transform.kwargs.items() if isinstance(val, ABCInput)
        ]
        outputs += [
            val for _, val in transform.kwargs.items() if isinstance(val, ABCOutput)
        ]

    static_assets = _generate_static_assets(inputs, outputs)
    return asset_fns, static_assets
