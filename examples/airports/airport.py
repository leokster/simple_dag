from simple_dag import transform, PandasDFInput, PandasDFOutput, BinaryOutput, schedule
import os
import pandas as pd
from matplotlib import pyplot as plt
import io

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


@transform(
    df=PandasDFInput(
        "https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv",
        sep=",",
        name="raw_airport_codes",
        description="Raw data for airport codes",
    ),
    output=PandasDFOutput(
        os.path.join(BASE_DIR, "data/curated/airport_codes.csv"),
        name="airport_codes",
    ),
)
def download_data(df, output: PandasDFOutput):
    output.write_data(df, index=False)


@schedule(on_upstream_success=True)
@transform(
    df=PandasDFInput(
        os.path.join(BASE_DIR, "data/curated/airport_codes.csv"), sep=","
    ),
    output=PandasDFOutput(
        os.path.join(BASE_DIR, "data/curated/airports_by_country.csv"),
        name="airports_by_country",
    ),
)
def create_airports_by_country(df, output: PandasDFOutput):
    df = df.groupby("iso_country")["ident"].count().reset_index()
    output.write_data(df, index=False)


# join full country names
@schedule(on_upstream_success=True)
@transform(
    df1=PandasDFInput(
        os.path.join(BASE_DIR, "data/curated/airports_by_country.csv"), sep=","
    ),
    df2=PandasDFInput(
        "https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv",
        sep=",",
        name="raw_country_codes",
        description="Raw data for country codes",
    ),
    output=PandasDFOutput(
        os.path.join(BASE_DIR, "data/curated/airports_by_country_full.csv"),
        name="airports_by_country_full",
    ),
)
def create_airports_by_country_full(df1, df2, output: PandasDFOutput):
    # df2 select only country code and country name
    df2 = df2[["ISO3166-1-Alpha-2", "CLDR display name"]]
    df = pd.merge(df1, df2, left_on="iso_country", right_on="ISO3166-1-Alpha-2")
    df = df.drop(columns=["ISO3166-1-Alpha-2"])
    output.write_data(df, index=False)


@schedule(on_upstream_success=True)
@transform(
    df=PandasDFInput(
        os.path.join(BASE_DIR, "data/curated/airports_by_country_full.csv"), sep=","
    ),
    image=BinaryOutput(
        os.path.join(BASE_DIR, "data/curated/airports_by_country.png"),
        name="airports_by_country_plot",
    ),
)
def create_airports_by_country_plot(df, image: BinaryOutput):
    fig, ax = plt.subplots()
    df = df.sort_values("ident", ascending=False).head(10)
    df.plot.barh(x="CLDR display name", y="ident", ax=ax)
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    buf.seek(0)
    image.write_data(buf.read())