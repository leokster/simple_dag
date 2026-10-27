=======
History
=======

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
