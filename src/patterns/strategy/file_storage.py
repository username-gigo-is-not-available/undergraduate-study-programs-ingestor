import logging
from io import BytesIO
from pathlib import Path

import aiohttp
import pandas as pd
from aiohttp import ClientResponse
from miniopy_async import S3Error, Minio

from src.clients import MinioClient
from src.configurations import StorageConfiguration


class FileStorageStrategy:
    async def read_data(self, input_file_name: Path) -> pd.DataFrame:
        raise NotImplementedError


class LocalFileStorage(FileStorageStrategy):

    async def read_data(self, input_file_name: Path) -> pd.DataFrame:
        try:
            return pd.read_csv(StorageConfiguration.INPUT_DIRECTORY_PATH / input_file_name)
        except FileNotFoundError as e:
            logging.error(f"File not found: {StorageConfiguration.INPUT_DIRECTORY_PATH / input_file_name}, {e}")
            raise
        except Exception as e:
            logging.error(f"Failed to read data from local storage: {e}")
            raise


class MinioFileStorage(FileStorageStrategy):

    async def read_data(self, input_file_name: Path) -> pd.DataFrame:
        try:
            input_file_name: str = str(input_file_name)
            async with aiohttp.ClientSession() as session:
                minio: Minio = MinioClient.connect()
                response: ClientResponse = await minio.get_object(
                    bucket_name=StorageConfiguration.MINIO_SOURCE_BUCKET_NAME,
                    object_name=str(input_file_name),
                    session=session,
                )
                csv_bytes: bytes = await response.read()
                csv_buffer: BytesIO = BytesIO(csv_bytes)
            return pd.read_csv(csv_buffer)
        except S3Error as e:
            logging.error(f"Failed to read data from MinIO bucket {StorageConfiguration.MINIO_SOURCE_BUCKET_NAME}: {e}")
            raise
