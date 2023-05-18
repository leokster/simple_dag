from simple_pipeline import get_dagster_assets
import os

TRANSFORM_DIR = os.path.dirname(os.path.abspath(__file__))
computed_assets, static_assets = get_dagster_assets(
    TRANSFORM_DIR,
)
