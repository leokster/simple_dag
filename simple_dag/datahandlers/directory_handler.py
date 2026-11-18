from simple_dag import storage
from simple_dag.datahandlers import base_handler
from simple_dag.utils import fsspec


class DirectoryInput(base_handler.ABCInput):
    """Hands the resolved path of a directory to the transform.

    ``get_data`` returns ``None`` when the path is not an existing directory.
    """

    asset_kind = "directory"
    data_format = "binary"
    default_extension = ""
    storage_layout = "directory"

    def __init__(
        self, path, *args, name=None, description=None, health_checks=None, **kwargs
    ) -> None:
        super().__init__(path, name, description, health_checks)
        self.args = args
        self.kwargs = kwargs

    def get_data(self):
        if not fsspec.isdir(self.path):
            return None
        path = storage.resolve(self.path)
        self.check(path, "input")
        return path


class DirectoryOutput(base_handler.ABCOutput):
    """Holds the path of a directory the transform fills itself via ``.path``."""

    asset_kind = "directory"
    data_format = "binary"
    default_extension = ""
    storage_layout = "directory"

    def write_data(self, data):
        raise NotImplementedError(
            "DirectoryOutput does not support write_data method, "
            "use directly the .path attribute"
        )
