"""Top-level package for simple_pipeline."""

__author__ = """Tim Rohner"""
__email__ = "info@timrohner.ch"
__version__ = "0.1.0"

from simple_pipeline.datahandlers.spark_handler import SparkDFInput, SparkDFOutput
from simple_pipeline.datahandlers.pandas_handler import PandasDFInput, PandasDFOutput
from simple_pipeline.datahandlers.binary_handler import BinaryInput, BinaryOutput
from simple_pipeline.transforms import transform, schedule
from simple_pipeline.dagster import get_dagster_assets

__all__ = [
    "SparkDFInput",
    "SparkDFOutput",
    "PandasDFInput",
    "PandasDFOutput",
    "BinaryInput",
    "BinaryOutput",
    "transform",
    "schedule",
    "get_dagster_assets",
]
