from __future__ import annotations

import uuid
import time
import boto3
import pandas as pd
from pathlib import Path

from pyllas.sql import infuse, load_query
from pyllas.utils import logger
from pyllas.storage.s3 import S3Client, S3Path
from pyllas.utils.progress_bar import ProgressBar


class Athena:
    """
        Athena client.

        Provides methods to execute SQL queries in AWS Athena.

        :param workgroup: name of the workgroup to execute queries
        :param s3_output_location: S3 path to store query results
        :param debug: enable logging debug level
    """

    def __init__(
            self,
            workgroup: str,
            s3_output_location: str,
            debug: bool = False,
    ):
        self._logger = logger.get_logger(name='pyllas.Athena', log_level='DEBUG' if debug else 'INFO')
        self._athena = boto3.client('athena')
        self._s3 = S3Client(debug=debug)

        self.workgroup = workgroup
        self.s3_output_location = S3Path(s3_output_location)

    def query(self, query: str | Path,
              *,
              params: dict = None,
              date_fields: tuple | list = ('date', 'event_date', 'report_date'),
              ask_status_sec: int = 5) -> pd.DataFrame:
        """
        Execute query and load results as pandas DataFrame.

        Parameters
        ----------
        :param query: str or Path
            query string or sql file path to read
        :param params: dict
            parameters to infuse :param query, see :func: pyllas.sql.infuse
        :param date_fields: tuple or list
               fields names to convert to pandas.datetime. Default: ('date', 'event_date', 'report_date')
        :param ask_status_sec: int
               interval in seconds to check query status. Default: 5
        """
        date_fields = tuple(date_fields if date_fields else ())
        query_name = self.create_table(query=query, params=params, ask_status_sec=ask_status_sec)
        query_path = self.s3_output_location / query_name

        self._logger.info('Load and transform query results.')
        with ProgressBar() as progress:
            dataframe = self._s3.read_orc(query_path, progress=progress)

        for date_field in date_fields:
            if date_field in dataframe:
                dataframe[date_field] = pd.to_datetime(dataframe[date_field])

        return dataframe

    def create_table(self, *, query: str, params: dict = None,
                     prefix: str = 'tmp_', name: str = None,
                     overwrite: bool = False, ask_status_sec: int = 5) -> str:
        """
        Create table with query results.

        Parameters
        ----------
        :param query: str or Path
            query string or sql file path to read
        :param params: dict
            parameters to infuse :param query, see :func: pyllas.sql.infuse
        :param name: str
            name for the table. Default: auto-generated name
        :param prefix: str
            prefix for the auto-generated table name, used if :param name is None. Default: `tmp_`
        :param overwrite: bool
            overwrite table if exists. Default: False
        :param ask_status_sec: int
            interval in seconds to check query status. Default: 5
        """
        query = load_query(query, params) if type(query) is Path else infuse(query, params)
        table_name = name or f'{prefix}{uuid.uuid4()}'.replace('-', '_')

        query_name = f'{self.workgroup}.{table_name}'
        query_path = self.s3_output_location / query_name

        if overwrite:
            self._logger.info(f'Overwriting table: `{query_name}` data...')
            self.__drop_table(query_name, ask_status_sec)
            self._s3.delete(query_path)

        query = f"""
          CREATE TABLE {query_name}
          WITH (
            format = 'ORC',
            external_location = '{query_path}'
          )
          AS 
          {query}
        """

        self._logger.debug(query)

        query_id = self._athena.start_query_execution(
            QueryString=query,
            WorkGroup=self.workgroup,
        )['QueryExecutionId']

        self._logger.info(f'Query with id `{query_id}` has been run')
        report = self.__wait_query_complete(query_id, ask_status_sec)

        self._logger.info(
            f"Athena execution time: {round(report['QueryExecution']['Statistics']['TotalExecutionTimeInMillis'] / 1000)} sec")
        self._logger.info(
            f"Data scanned: {round(report['QueryExecution']['Statistics']['DataScannedInBytes'] / (1024 ** 2))} mb")

        return query_name

    def execute_statement(self, query: str | Path, *, database: str,
                          params: dict = None, ask_status_sec: int = 5) -> None:
        """
        Execute queries that don't select data. Such as `CREATE TABLE`, `DROP TABLE` etc.

        Parameters
        ----------
        :param query: str or Path
            query string or sql file path to read
        :param database: str
            database name
        :param params: dict
            parameters to infuse :param query, see :func: pyllas.sql.infuse
        :param ask_status_sec: int
            interval in seconds to check query status. Default: 5
        """
        query = load_query(query, params) if type(query) is Path else infuse(query, params)

        query_id = self._athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            WorkGroup=self.workgroup
        )['QueryExecutionId']

        self.__wait_query_complete(query_id, ask_status_sec)

    def cancel_query(self, query_id: str) -> None:
        """
        Cancel query.

        Parameters
        ----------
        :param query_id: str
            query id to cancel
        """
        self._athena.stop_query_execution(QueryExecutionId=query_id)

    def __drop_table(self, table_name: str, ask_status_sec: int) -> None:
        self.execute_statement(
            database=self.workgroup,
            query=f'DROP TABLE IF EXISTS {table_name}',
            ask_status_sec=ask_status_sec
        )

    def __wait_query_complete(self, query_id: str, ask_status_sec: int):
        """Wait query to complete and return response."""
        with ProgressBar() as progress:
            while True:
                try:
                    response = self._athena.get_query_execution(QueryExecutionId=query_id)
                    status = response['QueryExecution']['Status']
                    state = status['State']

                    if state in ['QUEUED', 'RUNNING']:
                        time.sleep(ask_status_sec)
                        progress.tick()
                    elif state == 'SUCCEEDED':
                        return response
                    elif state == 'CANCELLED':
                        raise ValueError(f"Query was cancelled. The reason is {status['StateChangeReason']}")
                    elif state == 'FAILED':
                        raise ValueError(f"Query was failed. The reason is {status['StateChangeReason']}")

                except KeyboardInterrupt:
                    self._logger.info(f'Cancel query with id `{query_id}`')
                    self.cancel_query(query_id)
                    exit(0)
