from simple_dag.datahandlers import base_handler
import fsspec


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
        fs = fsspec.filesystem("file")

        # check if path is a directory
        if fs.isdir(self.path):
            return self.path


class DirectoryOutput(base_handler.ABCOutput):
    def __init__(self, path, name=None, description=None, health_checks=[]) -> None:
        self.path = path
        self.name = name
        self.description = description

    def write_data(self, data):
        raise NotImplementedError(
            "DirectoryOutput does not support write_data method, use directly the .path attribute"
        )
