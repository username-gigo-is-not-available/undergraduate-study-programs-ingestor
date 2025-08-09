import json
import logging
from io import BytesIO
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd
from aiohttp import ClientResponse
from fastavro import parse_schema, reader
from miniopy_async import S3Error, Minio

from src.clients import MinioClient
from src.configurations import StorageConfiguration


class StorageStrategy:

    async def load_schema(self, schema_file_name: Path) -> dict:
        raise NotImplementedError

    async def read_data(self, input_file_name: Path) -> pd.DataFrame:
        raise NotImplementedError


class LocalStorage(StorageStrategy):

    async def load_schema(self, schema_file_name: Path) -> str | list | dict | None:
        path: Path = StorageConfiguration.SCHEMA_DIRECTORY_PATH / schema_file_name
        try:
            logging.info(f"Reading schema from local storage: {path}")
            with open(path, "r", encoding="utf-8") as f:
                raw: dict = json.load(f)
                return parse_schema(raw)
        except OSError as e:
            logging.error(f"Failed to read schema from local storage: {path} {e}")
            return {}

    async def read_data(self, input_file_name: Path) -> pd.DataFrame:
        path: Path = StorageConfiguration.INPUT_DATA_DIRECTORY_PATH / input_file_name
        try:
            logging.info(f"Reading data from local storage {path}")
            with open(path, "rb") as f:
                records: list[dict[str, Any]] = list(reader(f))
            return pd.DataFrame(records)
        except FileNotFoundError as e:
            logging.error(f"File not found: {StorageConfiguration.INPUT_DATA_DIRECTORY_PATH / input_file_name}, {e}")
            raise
        except Exception as e:
            logging.error(f"Failed to read data from local storage: {e}")
            return pd.DataFrame()


class MinioStorage(StorageStrategy):

    async def load_schema(self, schema_file_name: Path) -> dict:
        object_name: str = "/".join([StorageConfiguration.MINIO_INPUT_DATA_BUCKET_NAME, str(schema_file_name)])
        try:
            logging.info(
                f"Reading schema from MinIO bucket: {StorageConfiguration.MINIO_SCHEMA_BUCKET_NAME}/{object_name}")
            async with aiohttp.ClientSession() as session:
                minio: Minio = MinioClient.connect()
                response: ClientResponse = await minio.get_object(
                    bucket_name=StorageConfiguration.MINIO_SCHEMA_BUCKET_NAME,
                    object_name=object_name,
                    session=session,
                )
                data: bytes = await response.read()
                buffer: BytesIO = BytesIO(data)
            return parse_schema(json.load(buffer))
        except S3Error as e:
            logging.error(f"Failed to read schema from MinIO bucket {StorageConfiguration.MINIO_SCHEMA_BUCKET_NAME}/{object_name}: {e}")
            return {}

    async def read_data(self, input_file_name: Path) -> pd.DataFrame:
        try:
            logging.info(f"Reading data from MinIO bucket: {StorageConfiguration.MINIO_INPUT_DATA_BUCKET_NAME}/{input_file_name}")
            input_file_name: str = str(input_file_name)
            async with aiohttp.ClientSession() as session:
                minio: Minio = MinioClient.connect()
                response: ClientResponse = await minio.get_object(
                    bucket_name=StorageConfiguration.MINIO_INPUT_DATA_BUCKET_NAME,
                    object_name=input_file_name,
                    session=session,
                )
                data: bytes = await response.read()
                buffer: BytesIO = BytesIO(data)
                return pd.DataFrame(list(reader(buffer)))
        except S3Error as e:
            logging.error(f"Failed to read data from MinIO bucket {StorageConfiguration.MINIO_INPUT_DATA_BUCKET_NAME}: {e}")
            return pd.DataFrame()