import os

from dagster import MetadataValue, asset, AssetIn, SourceAsset

from simple_pipeline.datahandlers.base_handler import ABCInput, ABCOutput
from simple_pipeline.transforms import find_transform_instances_in_folder

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


def get_asset_description(path):
    return PATH_TO_DESCRIPTION.get(path, "") + "\n\n" + f"Path: {path}"


def get_asset_name(path):
    return PATH_TO_NICE_NAME.get(path, _dataset_name_from_path(path))


def generate_static_assets(inputs, outputs):
    input_paths = [x.path for x in inputs]
    output_paths = [x.path for x in outputs]

    # get all input_paths which are not in output_paths
    # these are the static assets
    static_assets_paths = list(set(input_paths) - set(output_paths))

    assets = []
    for name in static_assets_paths:
        assets.append(
            SourceAsset(
                key=get_asset_name(name),
                description=get_asset_description(name),
            )
        )

    return assets


def generate_dagster_asset(transformer):
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

    print("Asset: ", get_asset_name(output.path))

    @asset(
        name=get_asset_name(output.path),
        non_argument_deps={get_asset_name(val.path) for _, val in inputs.items()},
        description=get_asset_description(output.path),
    )
    def asset_fn(**kwargs) -> None:
        transformer()

    return asset_fn


def get_dagster_assets(transforms_folder):
    all_transforms = find_transform_instances_in_folder(transforms_folder)

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
                        if val.name != potential_collision[0]:
                            raise ValueError(
                                f"Duplicate name {val.name} found for {val.path} and {potential_collision[0]}"
                            )
                    PATH_TO_NICE_NAME[val.path] = val.name

    for transform in all_transforms:
        asset_fns.append(generate_dagster_asset(transform))
        inputs += [
            val for _, val in transform.kwargs.items() if isinstance(val, ABCInput)
        ]
        outputs += [
            val for _, val in transform.kwargs.items() if isinstance(val, ABCOutput)
        ]

    static_assets = generate_static_assets(inputs, outputs)
    return asset_fns, static_assets
