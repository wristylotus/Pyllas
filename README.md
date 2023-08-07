What is Pyllas?
-------------
Pyllas is a Python library for interacting with AWS Athena.

It is designed for data analysis in Jupyter notebooks, but can be used in any Python environment.

Features:
* Easy to use.
* Good Performance even on large datasets.
* Query result as Pandas DataFrame.
* Create materialized tables from queries and use them in subsequent queries.
* Get information about query execution progress, time and data scanned.
* Automatically cancel queries when stop execution of Jupyter notebook cell or on KeyboardInterrupt.

Quick start
-----------

Pyllas can be installed using pip:

```bash
pip install pyllas
```

Here is a small example:

```python
import pyllas

athena = pyllas.Athena(
    workgroup='primary',
    s3_output_location='s3://aws-athena-query-results/primary/'
)

athena.query("SELECT 'Hello Athena!' AS greeting")
```

