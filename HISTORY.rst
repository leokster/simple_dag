=======
History
=======

1.4.0 (2026-11-18)
------------------

* One contract for every handler (``simple_dag.datahandlers.DataHandler``): the shared options
  ``name``, ``description`` and ``health_checks``, ``bind_path`` / ``bind_files`` /
  ``output_path`` to rebind a declaration to staged files, ``with_asset`` to tie it to a logical
  asset, and ``check_results`` with the outcome of its health checks. The Arrow handlers are
  ordinary handlers; definition manifests describe every handler (``kind``, ``format``,
  ``layout``, ``handler``) and read an absolute path as the asset path for all of them. A
  handler records that path as its ``asset_path`` when it is constructed.
* ``simple_dag.datahandlers`` exports every handler and the base classes; ``ArrowInput`` and
  ``ArrowOutput`` are also importable from ``simple_dag``.
* The contents of ``simple_dag.platform`` moved to ``simple_dag.checks``,
  ``simple_dag.definitions`` and ``simple_dag.execution``; ``simple_dag.platform`` keeps
  re-exporting the 1.3.0 names.
* Health checks: the binary and Spark handlers, ``PathInput`` and ``DirectoryInput`` now run them
  (they were accepted but ignored); a ``DirectoryOutput`` is checked with
  ``output.check(output.path, "output")`` by whoever fills it. A check passes on any truthy
  result, an Arrow scalar by its value (the Arrow handlers of 1.3.0 required the literal
  ``True``). All checks of a handler run, and the ``ValueError`` of a failed one reads
  ``Input checks failed for <path>: <check> (failed)`` instead of ``Health check failed for
  <path>: <check>``. A check that raises is logged and reported as ``error`` instead of
  propagating; ``severity = "warning"`` on a check records its outcome without stopping the
  transform.
* ``ExecutionContext`` gains ``secrets``, ``spark`` and ``env`` (the environment variables
  configured by the host). ``SparkDFInput`` reads through ``ctx.spark`` and treats a path without
  a known extension as Parquet.
* ``Multiple`` / ``MultipleIterator`` rebind a copy of the wrapped input to every file instead of
  constructing a new one, so options such as ``schema_validation`` reach every file and the
  ``kwargs`` of the wrapped input are no longer modified. A custom input that derives state from
  its path in ``__init__`` needs a ``bind_path`` of its own to be wrapped.
* Fixes: ``ctx`` is passed to a transform only if it is an argument of the function (a local
  variable of that name used to count), handlers no longer share one default ``health_checks``
  list, and ``ArrowOutput`` prefers DuckDB's ``to_arrow_table`` over the deprecated
  ``fetch_arrow_table``.

1.3.0 (2026-11-17)
------------------

* ``simple_dag.platform``, optional building blocks for a host that runs transforms:
  ``ArrowInput`` / ``ArrowOutput`` (Parquet as ``pyarrow`` tables, ``pip install
  simple_dag[arrow]``) with logical ``/project/path`` asset references, ``definition_manifest``
  and ``manifest_digest``, ``ExecutionContext`` and ``execute`` for isolated runs with copied
  bindings and several outputs, and structured check results (``CheckResult``,
  ``evaluate_check``).

1.2.2 (2026-11-02)
------------------

* Dagster: every asset carries definition metadata ``simple_dag/path`` (the handler path
  as written) and, when that path resolves to a URI, the standard ``dagster/uri``, so a
  platform can link assets to the files behind them.

1.2.1 (2026-11-01)
------------------

* Type the handler base classes (``path: str``, ``name``/``description`` optional) so
  downstream code such as ``Path(output.path)`` type-checks now that the package ships
  ``py.typed``.

1.2.0 (2026-11-01)
------------------

* Base-relative paths: ``simple_dag.configure(base=...)`` or ``SIMPLE_DAG_BASE`` resolves
  relative handler paths against one root (local folder, ``abfs://``, ``s3://``...).
  Paths below the base authenticate against Azure by default (``anon=False``) and local
  writes create missing parent directories. Absolute paths and URIs are unchanged.
* ``simple_dag.utils.fsspec``: ``list_files`` returns base-relative results for relative
  patterns; new ``exists``, ``isdir`` and ``remove`` helpers.
* ``@schedule(..., timezone=...)`` and ``find_transform_instances_in_package`` (import-based
  discovery that works inside zipped/pex deployments).
* Dagster: ``build_definitions`` returns a ``Definitions`` object with one
  ``ScheduleDefinition`` per cron (RUNNING by default), an automation-condition sensor for
  ``on_upstream_success`` transforms, asset groups from the project folder, and asset names
  sanitised to Dagster's ``[A-Za-z0-9_]``. Naming state is per build instead of module-global.
* ``__version__`` now comes from the installed distribution; logging replaces ``print``.

1.1.0 (2026-10-27)
------------------

* Require Python 3.10+ (dropped 3.9); declare support for 3.13 and 3.14.
* Bump dependency floors: dagster/dagster-webserver >=1.12, pandas >=2.2 (pandas 3 supported),
  matplotlib >=3.8, click >=8.1, fsspec >=2024.2, requests >=2.31; add explicit pydantic dependency.
* Port the dagster orchestrator to the current dagster API (``deps``, ``AutomationCondition``,
  ``FreshnessPolicy.cron``, ``AssetSpec``).
* Modernize packaging metadata (PEP 639 license), dev tooling and the release workflow.

0.1.0 (2023-05-18)
------------------

* First release on PyPI.
