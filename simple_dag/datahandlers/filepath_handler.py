from simple_dag.datahandlers import base_handler


class PathInput(base_handler.ABCInput):
    def __init__(
        self, path, *args, name=None, description=None, health_checks=[], **kwargs
    ) -> None:
        self.path = path
        self.name = name
        self.description = description
        self.args = args
        self.kwargs = kwargs

    def get_data(self):
        return self.path
