import json

from pydantic import BaseModel

from simple_dag.datahandlers import base_handler
from simple_dag.utils import fsspec


class JsonInput(base_handler.ABCInput):
    """Reads a JSON file, optionally validated against a pydantic model.

    With ``schema_validation`` the transform receives the validated model, or the
    plain parsed JSON if ``return_pydantic_model`` is False.
    """

    asset_kind = "document"
    data_format = "json"
    default_extension = ".json"

    def __init__(
        self,
        path,
        *args,
        name=None,
        description=None,
        health_checks=None,
        schema_validation: type[BaseModel] | None = None,
        return_pydantic_model=True,
        **kwargs,
    ) -> None:
        super().__init__(path, name, description, health_checks)
        self.args = args
        self.kwargs = kwargs
        self.schema_validation = schema_validation
        self.return_pydantic_model = return_pydantic_model

    def get_data(self):
        if self.path.endswith(".json"):
            data = {}
            with fsspec.open(self.path, "r") as f:
                data = json.load(f)

            pydantic_data = None
            # if schema_validation is not None check pydantic schema
            if self.schema_validation is not None:
                pydantic_data = self.schema_validation.model_validate(data)

            if pydantic_data is not None and self.return_pydantic_model:
                self.check(pydantic_data, "input")
                return pydantic_data

            elif not self.return_pydantic_model and pydantic_data is None:
                raise ValueError(
                    "You need to specify a pydantic model to validate the data "
                    f"in {self.path}"
                )

            self.check(data, "input")
            return data

        raise ValueError(f"File extension not supported: {self.path}, only .json")


class JsonOutput(base_handler.ABCOutput):
    """Writes a dict, a list or a pydantic model to a JSON file."""

    asset_kind = "document"
    data_format = "json"
    default_extension = ".json"

    def __init__(
        self,
        path,
        name=None,
        description=None,
        health_checks=None,
        schema_validation=None,
        **asset_options,
    ) -> None:
        super().__init__(path, name, description, health_checks, **asset_options)
        self.schema_validation = schema_validation

    def write_data(self, data, *args, **kwargs):
        if isinstance(data, BaseModel):
            json_data = data.model_dump()

        elif isinstance(data, dict) or isinstance(data, list):
            json_data = data

        else:
            raise ValueError(
                f"Data type not supported: {type(data)}, "
                "only pydantic models and dicts/lists"
            )

        self.check(data, "output")

        with fsspec.open(self.path, "w") as f:
            json.dump(json_data, f, indent=2, *args, **kwargs)
