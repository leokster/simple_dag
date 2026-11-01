"""Top-level package for simple_dag."""

from importlib import metadata as _metadata

__author__ = """Tim Rohner"""
__email__ = "info@timrohner.ch"

try:
    __version__ = _metadata.version("simple_dag")
except _metadata.PackageNotFoundError:
    __version__ = "0.0.0"

from simple_dag.storage import configure
from simple_dag.datahandlers.multiple import Multiple, MultipleIterator
from simple_dag.datahandlers.spark_handler import SparkDFInput, SparkDFOutput
from simple_dag.datahandlers.pandas_handler import (
    PandasDFInput,
    PandasDFOutput,
)
from simple_dag.datahandlers.binary_handler import BinaryInput, BinaryOutput
from simple_dag.datahandlers.filepath_handler import PathInput
from simple_dag.datahandlers.directory_handler import DirectoryInput, DirectoryOutput
from simple_dag.datahandlers.json_handler import JsonInput, JsonOutput
from simple_dag.transforms import transform, schedule, Transform

__all__ = [
    "BinaryInput",
    "BinaryOutput",
    "configure",
    "DirectoryInput",
    "DirectoryOutput",
    "JsonInput",
    "JsonOutput",
    "Multiple",
    "MultipleIterator",
    "PandasDFInput",
    "PandasDFOutput",
    "PathInput",
    "schedule",
    "SparkDFInput",
    "SparkDFOutput",
    "transform",
    "Transform",
]
