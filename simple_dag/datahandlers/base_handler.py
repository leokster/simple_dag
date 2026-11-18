"""Base classes of the data handlers.

A handler declares one input or output of a transform: where the data lives, how
it is read or written and which health checks guard it. A declaration can be
rebound to another physical location (``bind_path``, ``bind_files``) without
losing its options, which is how a runner stages inputs and outputs.
"""

import copy
import os
from pathlib import Path
from typing import Any, Callable, Sequence, TypeVar

from simple_dag.checks import BLOCKING, CheckResult, evaluate_check

_Handler = TypeVar("_Handler", bound="DataHandler")


class _Required:
    """Default of ``path``, which only a bare ``super().__init__()`` leaves out."""

    def __repr__(self) -> str:
        return "<required>"


_REQUIRED: Any = _Required()


def implied_asset_path(path: object, asset_id: str | None) -> str | None:
    """The logical asset a path stands for when none is declared.

    An absolute path, given as a string, names the asset itself, unless the
    handler is bound by id.
    """
    if isinstance(path, str) and path.startswith("/") and not asset_id:
        return path
    return None


class DataHandler:
    """What every input and output has in common.

    Args:
        path: where the data lives.
        name: asset name in an orchestrator, instead of one derived from the path.
        description: asset description in an orchestrator.
        health_checks: callables that receive the data and return something truthy.
        asset_id: identifier of the logical asset in a catalog, if it has one.
        asset_path: logical asset the data belongs to, when ``path`` is not it.
        schema_fields: declared columns, e.g.
            ``{"name": "total", "type": "double", "nullable": True}``.

    Subclasses describe their data with four class attributes:

    * ``asset_kind``: ``"file"``, ``"table"``, ``"document"`` or ``"directory"``
    * ``data_format``: ``"binary"``, ``"csv"``, ``"json"`` or ``"parquet"``
    * ``storage_layout``: ``"file"`` (one file), ``"dataset"`` (one file or a
      directory of part files), ``"directory"`` or ``"pattern"`` (a glob)
    * ``default_extension``: the suffix ``output_path`` uses when ``path`` has none

    The remaining class attributes are defaults: a subclass may set only some of
    them in a constructor of its own, without calling this one.
    """

    asset_kind = "file"
    data_format = "binary"
    storage_layout = "file"
    default_extension = ".bin"

    path: str
    name: str | None = None
    description: str | None = None
    asset_id: str | None = None
    asset_path: str | None = None
    health_checks: Sequence[Callable[[Any], Any]] = ()
    schema_fields: Sequence[dict] = ()
    check_results: Sequence[CheckResult] = ()

    def __init__(
        self,
        path: str = _REQUIRED,
        name: str | None = None,
        description: str | None = None,
        health_checks: Sequence[Callable[[Any], Any]] | None = None,
        *,
        asset_id: str | None = None,
        asset_path: str | None = None,
        schema_fields: Sequence[dict] | None = None,
    ) -> None:
        if path is _REQUIRED:
            # ``super().__init__()`` of a handler that sets its attributes itself,
            # as every handler did before this constructor existed.
            return
        self.path = path
        self.name = name
        self.description = description
        self.health_checks = health_checks if health_checks is not None else []
        self.asset_id = asset_id
        self.asset_path = asset_path or implied_asset_path(path, asset_id)
        self.schema_fields = schema_fields if schema_fields is not None else []
        self.check_results = []

    def bind_path(self: _Handler, path: str) -> _Handler:
        """A copy of this declaration at another physical path.

        Options, health checks and the logical asset are kept; check results
        start empty.
        """
        bound = copy.copy(self)
        bound.path = str(path)
        bound.check_results = []
        return bound

    def with_asset(
        self: _Handler, path: str, *, schema_fields: Sequence[dict] | None = None
    ) -> _Handler:
        """A copy of this declaration tied to the logical asset ``path``.

        The copy keeps reading or writing its own ``path``; the asset path, e.g.
        ``"/project/orders"``, is what a catalog knows the data by.
        """
        bound = self.bind_path(self.path)
        bound.asset_path = path
        if schema_fields is not None:
            bound.schema_fields = schema_fields
        return bound

    def check(self, data: Any, stage: str) -> None:
        """Run the health checks on ``data``; raise if a blocking one does not pass.

        The results stay available as ``check_results`` either way. ``stage`` is
        ``"input"`` or ``"output"``.
        """
        self.check_results = [
            evaluate_check(check, data, stage) for check in self.health_checks
        ]
        failed = [
            f"{result.check_id} ({result.status})"
            for result in self.check_results
            if result.status != "passed" and result.severity == BLOCKING
        ]
        if failed:
            raise ValueError(
                f"{stage.capitalize()} checks failed for {self.path}: "
                + ", ".join(failed)
            )


class ABCInput(DataHandler):
    """An input: ``get_data`` returns what the transform function receives."""

    def get_data(self) -> Any:
        """Load the data. Inputs override this; the base hands over ``None``."""
        return None

    def bind_files(self: _Handler, files: Sequence[str], root: str) -> _Handler:
        """A copy of this declaration reading ``files`` staged below ``root``.

        What the copy points at follows ``storage_layout``: the single file, the
        directory holding the part files of a dataset, ``root`` itself for a
        directory, or the declared glob pattern moved below ``root``.
        """
        if self.storage_layout == "directory":
            return self.bind_path(root)
        if self.storage_layout == "pattern":
            return self.bind_path(str(Path(root) / Path(self.path).name))
        if self.storage_layout == "dataset" and len(files) > 1:
            parents = [str(Path(file).parent) for file in files]
            return self.bind_path(os.path.commonpath(parents))
        if len(files) != 1:
            raise ValueError(
                f"{type(self).__name__} requires one file; "
                "use a directory or Multiple handler"
            )
        return self.bind_path(files[0])


class ABCOutput(DataHandler):
    """An output: the transform function receives it and calls ``write_data``."""

    def write_data(self, data: Any) -> None:
        """Store ``data``. Outputs override this; the base does nothing."""

    def output_path(self, root: str) -> str:
        """Where this output writes when it is staged below ``root``.

        A directory output gets ``root`` itself, anything else a file
        ``data.<ext>`` with the extension of the declared path.
        """
        if self.storage_layout == "directory":
            return str(Path(root))
        suffix = Path(self.path).suffix or self.default_extension
        return str(Path(root) / ("data" + suffix))
