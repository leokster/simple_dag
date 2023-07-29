from simple_dag import transform, PandasDFInput, PandasDFOutput, BinaryOutput, schedule
import os
import pandas as pd
from matplotlib import pyplot as plt
import io

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def check_salary(df):
    return "salary" in df.columns

@schedule("0 * * * *")
@transform(
    df=PandasDFInput(
        os.path.join(BASE_DIR, "data/raw/ds_salaries.csv"),
        sep=",",
        name="raw",
        description="Raw data upstream dataset",
        health_checks=[check_salary],
    ),
    output=PandasDFOutput(
        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023.csv"),
        name="saleries_2023",
    ),
)
def create_2023_salaries(df, output: PandasDFOutput):
    df = df[df["work_year"] == 2023]
    output.write_data(df, index=False)

@schedule(on_upstream_success=True)
@transform(
    df=PandasDFInput(
        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023.csv"),
        sep=",",
    ),
    output=PandasDFOutput(
        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023_ES.csv"),
        name="saleries_2023_ES",
    ),
)
def create_2023_salaries_ES(df, output: PandasDFOutput):
    df = df[df["company_location"] == "ES"]
    output.write_data(df, index=False)

@schedule(on_upstream_success=True)
@transform(
    df=PandasDFInput(
        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023.csv"), sep=","
    ),
    output=PandasDFOutput(
        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023_US.csv"),
        name="saleries_2023_US",
    ),
)
def create_2023_salaries_US(df, output: PandasDFOutput):
    df = df[df["company_location"] == "ES"]
    output.write_data(df, index=False)

@schedule(on_upstream_success=True)
@transform(
    df1=PandasDFInput(
        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023_US.csv"), sep=","
    ),
    df2=PandasDFInput(
        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023_ES.csv"), sep=","
    ),
    output=PandasDFOutput(
        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023_US_ES.csv"),
        name="saleries_2023_US_ES",
    ),
)
def create_2023_salaries_ES_US(df1, df2, output: PandasDFOutput):
    df = pd.concat([df1, df2])
    output.write_data(df, index=False)

@schedule(on_upstream_success=True)
@transform(
    df=PandasDFInput(
        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023_US_ES.csv"), sep=","
    ),
    image=BinaryOutput(
        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023_US_ES.png"),
        name="salerie_plot",
    ),
)
def create_2023_salaries_ES_US_plot(df, image: BinaryOutput):
    salaries = df.groupby("job_title")["salary"].mean()
    fig, ax = plt.subplots()
    salaries.plot.barh(ax=ax)
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    buf.seek(0)
    image.write_data(buf.read())


@schedule(on_upstream_success=True)
@transform(
    df=PandasDFInput(
        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023.csv"), sep=","
    ),
    output=PandasDFOutput(
        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023_remove_salery.csv"),
        health_checks=[check_salary],
        name="saleries_2023_remove_salery",
    ),
)
def create_2023_salaries_no_salary(df, output: PandasDFOutput):
    df = df.drop(columns=["salary"])
    output.write_data(df, index=False)

@schedule(on_upstream_success=True)
@transform(
    output=PandasDFOutput(
        os.path.join(BASE_DIR, "data/curated/random_numbers.csv"),
        health_checks=[check_salary],
        name="random_numbers",
    ),
)
def create_random_numbers(output: PandasDFOutput):
    df = pd.DataFrame({"random": [1, 2, 3, 4, 5], "salary": [100, 200, 300, 400, 500]})
    output.write_data(df, index=False)


if __name__ == "__main__":
    pass
