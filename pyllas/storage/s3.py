from __future__ import annotations

import io
import gzip
import boto3
import pandas as pd
from pyarrow import orc, concat_tables
from typing import Generator

from pyllas.storage.paths import S3Path
from pyllas.utils import logger
from pyllas.utils.progress_bar import ProgressBar


class S3Client:
    """S3 client for reading and writing data to S3.
    Takes arguments:
      - debug: if True, sets log level to DEBUG. Default: False.
    """

    def __init__(self, debug: bool = False):
        self._logger = logger.get_logger(name='pyllas.S3Client', log_level='DEBUG' if debug else 'INFO')
        self.s3 = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')

    def list_objects(self, path: S3Path, *, lazy=False) -> list[dict]:
        objects_seq = self.s3_resource.Bucket(path.bucket).objects.filter(Prefix=path.prefix)
        if lazy:
            return objects_seq

        return list(objects_seq)

    def list_paths(self, path: S3Path) -> Generator[S3Path, None, None]:
        return (S3Path(f's3://{ob.bucket_name}/{ob.key}') for ob in self.list_objects(path))

    def get_object(self, path: S3Path) -> dict:
        return self.s3.get_object(Bucket=path.bucket, Key=path.prefix)

    def read_object(self, object_path: S3Path, *, gzipped=False) -> bytes:
        """Reads object from S3 and returns it as bytes.
        Takes arguments:
          - object_path: path to the object in S3.
        """
        data = self.get_object(object_path)['Body'].read()

        return gzip.decompress(data) if gzipped else data

    def delete(self, path: S3Path) -> None:
        """Deletes all objects by path.
        Takes arguments:
          - path: path to the object or "folder" in S3.
        """
        self.list_objects(path, lazy=True).delete()

    def read_orc(self, path: S3Path, *, gzipped=False, progress: ProgressBar = None) -> pd.DataFrame:
        """Reads orc object(s) from S3 and returns result as a pyarrow.Table.
        Takes arguments:
          - path: path to the object(s) in S3.
          - gzipped: if True, decompresses data with gzip. Default: False.
        """

        def load_orc(object_path: S3Path):
            data = self.read_object(object_path, gzipped=gzipped)
            with io.BytesIO(data) as buffer:
                table = orc.ORCFile(buffer).read()
                if progress:
                    progress.tick()

                return table

        tables = list(map(load_orc, self.list_paths(path)))

        if tables:
            concat_tables(tables)
            return tables[0].to_pandas()
        else:
            return pd.DataFrame()
