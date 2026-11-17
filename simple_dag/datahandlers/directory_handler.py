from simple_dag import storage
from simple_dag.datahandlers import base_handler
from simple_dag.utils import fsspec


class DirectoryInput(base_handler.ABCInput):
    asset_kind = "directory"
    data_format = "binary"
    default_extension = ""
    storage_layout = "directory"

    def __init__(
        self, path, *args, name=None, description=None, health_checks=[], **kwargs
    ) -> None:
        self.path = path
        self.name = name
        self.description = description
        self.health_checks = health_checks
        self.args = args
        self.kwargs = kwargs

    def get_data(self):
        if fsspec.isdir(self.path):
            path = storage.resolve(self.path)
            self.check(path, "input")
            return path


class DirectoryOutput(base_handler.ABCOutput):
    asset_kind = "directory"
    data_format = "binary"
    default_extension = ""
    storage_layout = "directory"

    def __init__(self, path, name=None, description=None, health_checks=[]) -> None:
        self.path = path
        self.name = name
        self.description = description
        self.health_checks = health_checks

    def write_data(self, data):
        raise NotImplementedError(
            "DirectoryOutput does not support write_data method, use directly the .path attribute"
        )
