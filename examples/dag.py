from simple_dag.orchestrators.dagster import build_dagster_from_folder
import os

TRANSFORM_DIR = os.path.dirname(os.path.abspath(__file__))
computed_assets, static_assets = build_dagster_from_folder(
    TRANSFORM_DIR,
)
