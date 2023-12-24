# What is Pyllas?

Pyllas is a Python library for interacting with AWS Athena.

It is designed for data analysis in Jupyter notebooks, but can be used in any Python environment.

Features:

* Easy to use.
* Good Performance even on large datasets.
* Query result as Pandas DataFrame.
* Create materialized tables from queries and use them in subsequent queries.
* Get information about query execution progress, time and data scanned.
* Automatically cancel queries when stop execution of Jupyter notebook cell or on KeyboardInterrupt.

## Quick start

---

Pyllas can be installed using pip:

```bash
pip install pyllas
```

### Setup AWS environment

First, you need to add your AWS credentials to the file `~/.aws/credentials`.

> See [IAM credentials](https://us-east-1.console.aws.amazon.com/iam/home?region=us-east-1#/security_credentials?section=IAM_credentials)
for more information.
> 
Second, create bucket for Athena query results.
For example: `s3://wristylotus.athena/default-query-result/`

Third, create an Athena workgroup and schema for your tables.
For example, workgroup: `wristylotus`, schema: `create schema wristylotus`.
> [!IMPORTANT]
> Athena schema and workgroup must have the same name.

Finally, you need to grant the following permissions:

- `s3:*` on Athena queries result S3 path
- `athena:StartQueryExecution`
- `athena:CancelQueryExecution`
- `athena:GetQueryExecution`
- `glue:CreateTable`

Congratulations! You are ready to use Pyllas.

```python
import pyllas

athena = pyllas.Athena(
    workgroup='wristylotus',
    s3_output_location='s3://wristylotus.athena/default-query-result/'
)

athena.query("SELECT 'Hello Athena!' AS greeting")
```

## Documentation

---

### Initialization

An Athena client can be obtained using the `pyllas.Athena` initializer.

```python
import pyllas

athena = pyllas.Athena(
    # Athena workgroup name
    workgroup='wristylotus',
    # S3 path for Athena query results
    s3_output_location='s3://wristylotus.athena/default-query-result/',
    # Number of threads to load query results
    n_jobs=1,
    # Prints additional information about query execution
    debug=False
)
```

### Executing queries

To execute a query, use the `query` method:

```python
users_df = athena.query('SELECT id, name FROM users')
```

Pyllas also supports parameterized queries:

```python
users_df = athena.query('''
  SELECT id, name 
  FROM users 
  WHERE name IN ${names} AND age > ${age}
  ''', 
  params={'names': ['Bart', 'Lisa'], 'age': 14}
)
```

For the SQL templating use `Expr` and `Table`, `Database`, `Schema` type aliases:

```python
from pyllas import sql

users_df = athena.query('SELECT id, name FROM ${schema}.${table} ${filter}', 
  params={
      'schema': sql.Schema('main'), 
      'table': sql.Table('users'), 
      'filter': sql.Expr("WHERE name = 'Bart'")
  }
)
```
> For more information, see [API](#query-execution)

Instead of getting the result as a Pandas DataFrame, you can get the result table name:

```python
users_table = athena.create_table('SELECT id, name FROM users WHERE age > 14')
# and then use it in subsequent queries
athena.query(f'SELECT * FROM {users_table}')
```
> For more information, see [API](#create-table)

For a not SELECT queries, such as `CREATE TABLE`, `DROP TABLE` etc., use:

```python
athena.execute_statement("DROP TABLE users")
```
> For more information, see [API](#execute-statement)

## API Reference

---

### Client

The Athena class is a facade to all functionality offered by the library.

```python
class Athena:
    """
        Athena client.

        Provides methods to execute SQL queries in AWS Athena service.

        :param workgroup: name of the workgroup to execute queries
        :param s3_output_location: S3 path to store query results
        :param n_jobs: number of parallel jobs to read query results, default: 1.
               n_jobs=1, then use only main-threaded
               n_jobs=-1, then use all available CPUs
        :param debug: enable logging debug level
    """

    def __init__(
            self,
            workgroup: str,
            s3_output_location: str,
            n_jobs: int = 1,
            debug: bool = False
    ):
        ...
```
#### Query execution
```python
    def query(self, query: str | Path,
              *,
              params: dict = None,
              date_fields: Union[tuple, list] = ('date', 'event_date', 'report_date'),
              ask_status_sec: int = 5) -> pd.DataFrame:
        """
        Execute query and load results as pandas DataFrame.

        Parameters
        ----------
        :param query: str or pathlib.Path
            query string or sql file path to read
        :param params: dict
            parameters to infuse :param query, see :func: pyllas.sql.infuse
        :param date_fields: tuple or list
               field names to convert to pandas.datetime. Default: ('date', 'event_date', 'report_date')
        :param ask_status_sec: int
               interval in seconds to check query status. Default: 5
        """
```
#### Create table
```python
    def create_table(self, *, query: Path | str, params: dict = None,
                     prefix: str = 'tmp_', name: str = None,
                     overwrite: bool = False, ask_status_sec: int = 5) -> str:
        """
        Create a table with query results and return it name.

        Parameters
        ----------
        :param query: str or pathlib.Path
            query string or sql file path to read
        :param params: dict
            parameters to infuse :param query, see :func: pyllas.sql.infuse
        :param name: str
            name for the table. Default: auto-generated random name
        :param prefix: str
            prefix for the auto-generated table name, used if :param name is None. Default: `tmp_`
        :param overwrite: bool
            overwrite table if it exists. Default: False
        :param ask_status_sec: int
            interval in seconds to check query status. Default: 5
        """
```
#### Execute statement
```python
    def execute_statement(self, query: str | Path, *, database: str = None,
                          params: dict = None, batch_size: int = 1000, ask_status_sec: int = 5) -> PageIterator:
        """
        For all queries except SELECT. Such as `CREATE TABLE`, `DROP TABLE` etc.
        Returns PageIterator of dictionaries with query results.
        Example:
        >> athena.execute_statement("SHOW TABLES IN test_db", database='test_db')
        {'ResultSet': {'Rows': [{'Data': [{'VarCharValue': 'test_table'}]}], 'ResultSetMetadata': {'ColumnInfo': [{'CatalogName': 'hive', ...}]}}}

        Parameters
        ----------
        :param query: str or pathlib.Path
            query string or sql file path to read
        :param database: str
            database name
        :param params: dict
            parameters to infuse :param query, see :func: pyllas.sql.infuse
        :param batch_size: int
            batch size to read query results. Default: 1000
        :param ask_status_sec: int
            interval in seconds to check query status. Default: 5
        """
```
#### Cancel query
```python
    def cancel_query(self, query_id: str) -> None:
        """
        Cancel query.

        Parameters
        ----------
        :param query_id: str
            query id to cancel
        """
```