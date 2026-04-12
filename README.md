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
| `PandasDFInput(path, **kwargs)` | Reads a CSV via fsspec; kwargs forwarded to `pd.read_csv`; supports `health_checks` |
| `BinaryInput(path)` | Reads raw bytes; works with any fsspec-compatible path |
| `SparkDFInput(path)` | Spark DataFrame from JSON/Parquet/CSV (experimental) |
| `JsonInput(path, schema_validation=None)` | Reads a JSON file; validates with a Pydantic model if `schema_validation` is set; returns model or raw dict |
| `PathInput(path)` | Pass-through — function receives the raw path string |
| `DirectoryInput(path)` | Validates path is a directory; function receives the path string |
| `Multiple(input_obj)` | Expands a wildcard path into a **list** of loaded inputs |
| `MultipleIterator(input_obj)` | Like `Multiple` but returns a **lazy iterator** (memory-efficient) |

## Output Types

| Class | Description |
|---|---|
| `PandasDFOutput(path)` | Writes a DataFrame to CSV |
| `BinaryOutput(path)` | Writes raw bytes |
| `SparkDFOutput(path)` | Writes a Spark DataFrame to Parquet (experimental) |
| `JsonOutput(path)` | Writes a dict, list, or Pydantic model to JSON |
| `DirectoryOutput(path)` | Holds a path; `write_data` raises `NotImplementedError` — use `.path` directly |

## Health Checks

Pass a list of callables to any input or output. Each receives the loaded data and must return `True` to pass:

```python
def has_salary_col(df):
    return "salary" in df.columns

PandasDFInput("data.csv", health_checks=[has_salary_col])
```

A failed health check raises `ValueError`.

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
# dag.py
from simple_dag.orchestrators.dagster import build_dagster_from_folder
import os

computed_assets, static_assets = build_dagster_from_folder(
    os.path.dirname(os.path.abspath(__file__))
)
```

Launch the Dagster UI:

```bash
dagster dev -f dag.py
```

## Cloud Storage

All path arguments accept fsspec-compatible URIs — no code changes needed:

- `s3://my-bucket/path/to/data.csv`
- `abfs://my-container/path/to/data.csv` (Azure Blob)
- `/local/path/to/data.csv`

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
