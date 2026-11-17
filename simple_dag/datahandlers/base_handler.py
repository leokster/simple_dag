"""Common storage binding contract; rebinding preserves handler behavior."""

import copy
import os
from pathlib import Path
from typing import Any
from simple_dag.checks import evaluate_check


class DataHandler:
    path: str
    name: str | None = None
    description: str | None = None
    asset_path: str | None = None
    asset_kind = "file"
    data_format = "binary"
    storage_layout = "file"
    default_extension = ".bin"

    def bind_path(self, path: str):
        """Copy this declaration with a physical path, keeping options and validators."""
        bound = copy.copy(self)
        bound.path = str(path)
        bound.check_results = []
        return bound

    def with_asset(self, path: str, *, schema_fields=None):
        """Associate a logical asset without changing standalone storage behavior."""
        bound = self.bind_path(self.path)
        bound.asset_path = path
        if schema_fields is not None:
            bound.schema_fields = schema_fields
        return bound

    def check(self, data, stage):
        self.check_results = [
            evaluate_check(c, data, stage) for c in getattr(self, "health_checks", [])
        ]
        if any(c.status != "passed" and c.severity == "blocking" for c in self.check_results):
            raise ValueError(f"{stage.capitalize()} checks failed for {self.path}")

    def output_path(self, root: str):
        if self.storage_layout == "directory":
            return str(Path(root))
        suffix = Path(self.path).suffix or self.default_extension
        return str(Path(root) / ("data" + suffix))


class ABCInput(DataHandler):
    def bind_files(self, files: list[str], root: str):
        if self.storage_layout == "directory":
            return self.bind_path(root)
        if self.storage_layout == "pattern":
            return self.bind_path(str(Path(root) / Path(self.path).name))
        if self.storage_layout == "dataset" and len(files) > 1:
            return self.bind_path(os.path.commonpath([str(Path(f).parent) for f in files]))
        if len(files) != 1:
            raise ValueError(
                f"{type(self).__name__} requires one file; use a directory or Multiple handler"
            )
        return self.bind_path(files[0])

    def get_data(self) -> Any:
        raise NotImplementedError


class ABCOutput(DataHandler):
    def write_data(self, data: Any) -> None:
        raise NotImplementedError
