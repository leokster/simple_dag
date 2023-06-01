from simple_dag.datahandlers import base_handler


class BinaryInput(base_handler.ABCInput):
    def __init__(
        self, path, *args, name=None, description=None, health_checks=[], **kwargs
    ) -> None:
        self.path = path
        self.name = name
        self.description = description
        self.args = args
        self.kwargs = kwargs

    def get_data(self):
        with open(self.path, "rb") as f:
            return f.read()


class BinaryOutput(base_handler.ABCOutput):
    def __init__(self, path, name=None, description=None, health_checks=[]) -> None:
        self.path = path
        self.name = name
        self.description = description

    def write_data(self, data):
        with open(self.path, "wb") as f:
            f.write(data)
