from typing import Any


class ABCInput:
    path: str
    name: str | None = None
    description: str | None = None

    def get_data(self) -> Any:
        pass


class ABCOutput:
    path: str
    name: str | None = None
    description: str | None = None

    def write_data(self, data: Any) -> None:
        pass
