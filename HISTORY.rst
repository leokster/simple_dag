=======
History
=======

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
