===============
simple_dag
===============


.. image:: https://img.shields.io/pypi/v/simple_dag.svg
        :target: https://pypi.python.org/pypi/simple_dag

.. image:: https://img.shields.io/travis/leokster/simple_dag.svg
        :target: https://travis-ci.com/leokster/simple_dag

.. image:: https://readthedocs.org/projects/simple-pipeline/badge/?version=latest
        :target: https://simple-pipeline.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/leokster/simple_dag/shield.svg
     :target: https://pyup.io/repos/github/leokster/simple_dag/
     :alt: Updates



The easiest way to create simple pipeline in a orchstration agnostic way!

Just decorate your functions with `@transform`!

* Free software: MIT license


Getting Started:

.. code-block:: bash

    git clone https://github.com/leokster/simple_dag.git
    pip install simple_dag
    dagit -f examples/dag.py

Example:
--------

Assume you have a dataset of salaries for data scientists and you want to create a pipeline that filters the data to only include salaries for 2023.

You have the follwoing python script:

.. code-block:: python

    import os
    import pandas as pd

    def create_2023_salaries(df):
        df = df[df["work_year"] == 2023]
        return df

    if __name__ == "__main__":
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        input_df = pd.read_csv(os.path.join(BASE_DIR, "data/raw/ds_salaries.csv"), sep=",")
        df = create_2023_salaries(input_df)
        df.to_csv(os.path.join(BASE_DIR, "data/processed/2023_salaries.csv"), index=False)



with simple_dag your script will look like this:



.. code-block:: python

        import os
        import pandas as pd
        from simple_dag import transform, PandasDFInput, PandasDFOutput
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        @transform(
                df=PandasDFInput(
                        os.path.join(BASE_DIR, "data/raw/ds_salaries.csv"),
                        sep=",",
                ),
                output=PandasDFOutput(
                        os.path.join(BASE_DIR, "data/curated/ds_salaries_2023.csv"),
                ),
        )
        def create_2023_salaries(df, output: PandasDFOutput):
                df = df[df["work_year"] == 2023]
                output.write_data(df, index=False)
        
        if __name__ == "__main__":
                create_2023_salaries()








* TODO

