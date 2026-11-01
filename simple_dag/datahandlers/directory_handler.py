from simple_dag import storage
from simple_dag.datahandlers import base_handler
from simple_dag.utils import fsspec


class DirectoryInput(base_handler.ABCInput):
    def __init__(
        self, path, *args, name=None, description=None, health_checks=[], **kwargs
    ) -> None:
        self.path = path
        self.name = name
        self.description = description
        self.args = args
        self.kwargs = kwargs

    def get_data(self):
        if fsspec.isdir(self.path):
            return storage.resolve(self.path)


class DirectoryOutput(base_handler.ABCOutput):
    def __init__(self, path, name=None, description=None, health_checks=[]) -> None:
        self.path = path
        self.name = name
        self.description = description

    def write_data(self, data):
        raise NotImplementedError(
            "DirectoryOutput does not support write_data method, "
            "use directly the .path attribute"
        )
