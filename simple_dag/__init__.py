"""Top-level package for simple_dag."""

from importlib import metadata as _metadata

__author__ = """Tim Rohner"""
__email__ = "info@timrohner.ch"

try:
    __version__ = _metadata.version("simple_dag")
except _metadata.PackageNotFoundError:
    __version__ = "0.0.0"

from simple_dag.storage import configure
from simple_dag.datahandlers import (
    ArrowInput,
    ArrowOutput,
    BinaryInput,
    BinaryOutput,
    DirectoryInput,
    DirectoryOutput,
    JsonInput,
    JsonOutput,
    Multiple,
    MultipleIterator,
    PandasDFInput,
    PandasDFOutput,
    PathInput,
    SparkDFInput,
    SparkDFOutput,
)
from simple_dag.transforms import transform, schedule, Transform

__all__ = [
    "ArrowInput",
    "ArrowOutput",
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
