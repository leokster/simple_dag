class ABCInput:
    path = None
    name = None
    description = None

    def read_data(self):
        pass


class ABCOutput:
    path = None
    name = None
    description = None

    def write_data(self, data):
        pass
