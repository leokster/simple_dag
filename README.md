# simple_dag

![pypi](https://img.shields.io/pypi/v/simple_dag.svg)
[![Documentation Status](https://readthedocs.org/projects/simple-pipeline/badge/?version=latest)](https://simple-pipeline.readthedocs.io/en/latest/?version=latest)
[![Updates](https://pyup.io/repos/github/leokster/simple_dag/shield.svg)](https://pyup.io/repos/github/leokster/simple_dag/)

Orchestration-agnostic Python pipeline library. Decorate functions with `@transform` to define DAG nodes. Run them directly as plain Python callables or integrate with Dagster — no rewriting required.

- Free software: MIT license

## Getting Started

```bash
pip install simple_dag
```

```python
import os
from simple_dag import transform, PandasDFInput, PandasDFOutput

@transform(
    df=PandasDFInput("data/salaries.csv"),
    output=PandasDFOutput("data/salaries_2023.csv"),
)
def filter_2023(df, output: PandasDFOutput):
    df = df[df["work_year"] == 2023]
    output.write_data(df, index=False)

# Run directly
filter_2023()
```

## The Main Ideas

**What is a transform?**
A function decorated with `@transform`. Input arguments are loaded automatically; output arguments are passed through so your function can call `write_data()`.

**Orchestration-agnostic?**
Transforms are plain callables. Call them directly in Python or register them with Dagster using `build_dagster_from_folder`. Switch orchestrators without rewriting pipelines.

## `@transform`

```python
from simple_dag import transform, PandasDFInput, PandasDFOutput

@transform(
    df=PandasDFInput("data/raw.csv"),
    output=PandasDFOutput("data/processed.csv"),
)
def my_step(df, output: PandasDFOutput):
    output.write_data(df[df["value"] > 0], index=False)
```

- Keyword arguments that are `Input` instances are loaded and the result is passed to your function.
- Keyword arguments that are `Output` instances are passed through unchanged so you can call `write_data()`.

## `@schedule`

Stack `@schedule` above `@transform` to attach scheduling metadata:

```python
from simple_dag import transform, schedule, PandasDFInput, PandasDFOutput

@schedule("0 * * * *")              # cron: run every hour
@transform(...)
def hourly_step(...): ...

@schedule(on_upstream_success=True) # trigger when upstream finishes
@transform(...)
def downstream_step(...): ...
```

## Input Types

| Class | Description |
|---|---|
| `PandasDFInput(path, **kwargs)` | Reads a CSV via fsspec; kwargs forwarded to `pd.read_csv` |
| `ArrowInput(path)` | Reads a Parquet file or a directory of Parquet files into a `pyarrow.Table` (`pip install simple_dag[arrow]`) |
| `BinaryInput(path)` | Reads raw bytes; works with any fsspec-compatible path |
| `SparkDFInput(path)` | Spark DataFrame from JSON/Parquet/CSV (experimental) |
| `JsonInput(path, schema_validation=None)` | Reads a JSON file; validates with a Pydantic model if `schema_validation` is set; returns model or raw dict |
| `PathInput(path)` | Pass-through — function receives the raw path string |
| `DirectoryInput(path)` | Function receives the path string if it is a directory, `None` otherwise |
| `Multiple(input_obj)` | Expands a wildcard path into a **list** of loaded inputs |
| `MultipleIterator(input_obj)` | Like `Multiple` but returns a **lazy iterator** (memory-efficient) |

## Output Types

| Class | Description |
|---|---|
| `PandasDFOutput(path)` | Writes a DataFrame to CSV |
| `ArrowOutput(path)` | Writes a `pyarrow.Table` or a DuckDB relation to Parquet (`pip install simple_dag[arrow]`) |
| `BinaryOutput(path)` | Writes raw bytes |
| `SparkDFOutput(path)` | Writes a Spark DataFrame to Parquet (experimental) |
| `JsonOutput(path)` | Writes a dict, list, or Pydantic model to JSON |
| `DirectoryOutput(path)` | Holds a path; `write_data` raises `NotImplementedError` — use `.path` directly |

All handlers are importable from `simple_dag` and share the optional arguments `name` and
`description` (the asset's name and description in Dagster) and `health_checks`; `Multiple` takes
them from the input it wraps. `simple_dag.datahandlers` exports the handlers too, together with the
base classes `ABCInput` and `ABCOutput` for writing your own.

## Health Checks

Pass a list of callables to any input or output. Each receives the data, as loaded by the input or
as about to be written by the output, and returns something truthy to pass:

```python
def has_salary_col(df):
    return "salary" in df.columns

PandasDFInput("data.csv", health_checks=[has_salary_col])
```

A check that returns something falsy, or raises, fails. A failed check raises a `ValueError` that
names it; an output is not written in that case. To record a check without stopping the transform,
mark it as a warning:

```python
has_salary_col.severity = "warning"
```

After a read or write the handler keeps the outcomes as `check_results`: one
`simple_dag.checks.CheckResult` per check with its `check_id` (the function name unless the function
has a `check_id` attribute), `status` (`passed`, `failed` or `error`), `stage`, `severity` and
`duration_ms`.

Two handlers work differently. A `Multiple` runs the checks of the input it wraps on every file and
keeps no results itself. A `DirectoryOutput` has nothing to write, so its checks run when you call
`output.check(output.path, "output")` after filling the directory.

## Direct Execution

Transforms are plain callables — no Dagster required:

```python
filter_2023()   # loads inputs, runs function, writes outputs
```

You can also discover and run all transforms in a folder:

```python
from simple_dag.transforms import find_transform_instances_in_folder

transforms = find_transform_instances_in_folder("my_pipeline/")
for t in transforms:
    t()
```

## Dagster Integration

```python
# definitions.py
from simple_dag.orchestrators.dagster import build_definitions

defs = build_definitions("my_project.pipelines")  # package name, module, folder or list
```

`build_definitions` turns every transform into an asset (grouped by the first folder of a
base-relative path), every `@schedule(cron=...)` into a `ScheduleDefinition` that is running
from the start, and `on_upstream_success` transforms into an automation-condition sensor.
Pass `running=False` to create schedules in the stopped state.

Launch the Dagster UI:

```bash
dagster dev -m my_project.definitions
```

The older `build_dagster_from_folder(folder)` / `build_dagster(transforms)` helpers still
return `(assets, source_assets)` for hand-built `Definitions`.

## Cloud Storage

All path arguments accept fsspec-compatible URIs — no code changes needed:

- `s3://my-bucket/path/to/data.csv`
- `abfs://my-container/path/to/data.csv` (Azure Blob)
- `/local/path/to/data.csv`

## Base Path

Most pipelines read and write below one root. Configure it once and use relative paths in
the handlers; the same code then runs against a local folder on a laptop and against a
bucket or an Azure container in production:

```bash
export SIMPLE_DAG_BASE=~/data                                   # laptop
export SIMPLE_DAG_BASE=abfs://data@account.dfs.core.windows.net  # production
```

```python
from simple_dag import JsonInput, JsonOutput, transform

@transform(
    orders=JsonInput("sales/raw/orders.json"),
    out=JsonOutput("sales/curated/orders.json"),
)
def clean_orders(orders, out):
    out.write_data([o for o in orders if o["amount"] > 0])
```

- `simple_dag.configure(base=..., storage_options=...)` does the same from code;
  `SIMPLE_DAG_STORAGE_OPTIONS` takes a JSON object of fsspec options for the base.
- Below the base, Azure is accessed authenticated (`anon=False`, i.e. `DefaultAzureCredential`:
  workload identity, managed identity or `az login`) and local writes create missing parent
  directories. Absolute paths and URIs behave as before.
- Relative paths also give stable, base-independent asset names in Dagster
  (`sales/curated/orders.json` → `sales_curated_orders_json`, group `sales`).
- `simple_dag.utils.fsspec.open / exists / remove / list_files` apply the same resolution for
  code outside the handlers.
- The Arrow and Spark handlers hand their path to pyarrow and Spark as written: local paths and the
  URIs those libraries understand, without resolving against the base.

## Examples

### JSON + Pydantic

```python
from pydantic import BaseModel
from simple_dag import transform, JsonInput, JsonOutput

class SensorReading(BaseModel):
    sensor_id: str
    value: float

@transform(
    reading=JsonInput("data/reading.json", schema_validation=SensorReading),
    output=JsonOutput("data/alert.json"),
)
def evaluate(reading: SensorReading, output: JsonOutput):
    result = {"sensor_id": reading.sensor_id, "triggered": reading.value > 100}
    output.write_data(result)
```

### Multiple files with wildcard

```python
from simple_dag import transform, Multiple, PandasDFInput, PandasDFOutput

@transform(
    dfs=Multiple(PandasDFInput("data/raw/*.csv")),
    output=PandasDFOutput("data/combined.csv"),
)
def combine(dfs, output: PandasDFOutput):
    import pandas as pd
    output.write_data(pd.concat(dfs), index=False)
```

See `examples/ds_salaries/` for a full multi-step pipeline including schedules, health checks, and a plot output.

## Running transforms from a host

A scheduler, a data platform or a test harness can describe transforms, stage their data wherever
it likes and run them, without the pipeline code knowing about it. All of this is optional:
transforms stay plain callables.

```python
import pyarrow as pa
from simple_dag import ArrowInput, ArrowOutput, transform

@transform(
    orders=ArrowInput("/finance/raw/orders"),
    revenue=ArrowOutput(
        "/analytics/reports/revenue",
        schema_fields=[{"name": "total", "type": "double", "nullable": True}],
    ),
)
def summarize(orders, revenue, ctx):
    total = sum(orders.column("amount").to_pylist()) * ctx.parameters.get("factor", 1)
    revenue.write_data(pa.table({"total": [total]}))
```

### Definition manifest

`definition_manifest` describes transforms as plain data, so a host can register them without
keeping project code imported:

```python
from simple_dag.definitions import definition_manifest, manifest_digest

manifest = definition_manifest([summarize])  # or find_transform_instances_in_package(...)
manifest_digest(manifest)                    # same definitions, same digest
```

For every transform it lists the name, the source file and line, the schedules and their timezone,
and for every input and output the path, the handler class, the kind of data (`table`, `document`,
`file` or `directory`), its format, the declared `schema_fields` and the ids of the health checks
that block.

### Logical assets

An absolute handler path such as `/analytics/reports/revenue` names a logical asset: a reference a
catalog resolves (`/<project>/<path>`), not necessarily a place on disk. The manifest reports it as
`asset_path`. To read or write a real path and still name the asset, tie a copy of any handler to
it:

```python
PandasDFInput("data/orders.csv", sep=";").with_asset("/finance/raw/orders")
```

Outputs and `ArrowInput` also take `asset_path=` and `schema_fields=` as keyword arguments, and
`asset_id=` for a catalog that binds by identifier instead of by path.

### Binding and executing

Handlers are declarations. A host rebinds copies of them to the files it staged and runs the
transform with the copies; the declared transform is left untouched, so one process can run several
attempts side by side:

```python
from simple_dag.execution import ExecutionContext, execute

inputs = {
    "orders": summarize.inputs["orders"].bind_files(
        ["/work/orders/part-0.parquet"], "/work/orders"
    )
}
declared = summarize.outputs["revenue"]
outputs = {"revenue": declared.bind_path(declared.output_path("/work/out/revenue"))}

context = ExecutionContext(
    run_id="run-1",
    attempt_id="attempt-1",
    project_id="analytics",
    branch_id="main",
    environment_id="production",
    parameters={"factor": 2},
)
execute(summarize, context, inputs, outputs)  # writes /work/out/revenue/data.parquet
outputs["revenue"].check_results              # outcome of the output's health checks
```

- `bind_path(path)` replaces only the path; reader options, pydantic models and health checks stay.
- `bind_files(files, root)` picks what an input needs from the files staged below `root`: the file
  itself, the directory holding a multi-file dataset, `root` for a directory input or the glob of a
  `Multiple`.
- `output_path(root)` is where an output writes below `root`: `data.<ext>` with the declared
  extension, or `root` itself for a directory output. The host creates `root`.
- `execute` requires a binding for every declared input and output; a transform may have several
  outputs.

### Execution context

A transform that declares a `ctx` argument receives the `ExecutionContext` of its run: `run_id`,
`attempt_id`, `project_id`, `branch_id`, `environment_id`, `parameters`, `configuration` and
`partition`, plus `ctx.env` (the environment variables the host configured, a copy of
`configuration["env"]`), `ctx.secrets` (values the host supplies, left out of the context's
representation) and `ctx.spark` (a Spark session for `SparkDFInput`, if the host provides one).
simple_dag does not fetch credentials or start services for a context itself. Called directly,
without a host, a transform gets the Spark session as `ctx` when PySpark is installed and `None`
otherwise, so run a `ctx`-reading transform locally through `execute` with a context of your own.

`simple_dag.platform`, where 1.3.0 introduced these building blocks, still re-exports them.
