import pandas as pd

from src.configurations import StorageConfiguration, DatasetConfiguration
from src.patterns.strategy.storage import LocalStorage, MinioStorage


class StorageMixin:
    def __init__(self):
        if StorageConfiguration.FILE_STORAGE_TYPE == 'LOCAL':
            self.storage_strategy = LocalStorage()
        elif StorageConfiguration.FILE_STORAGE_TYPE == 'MINIO':
            self.storage_strategy = MinioStorage()
        else:
            raise ValueError(f"Unsupported storage type: {StorageConfiguration.FILE_STORAGE_TYPE}")

    async def load_schema(self, configuration: DatasetConfiguration) -> dict:
        return await self.storage_strategy.load_schema(configuration.schema_configuration.file_name)

    async def read_data(self, configuration: DatasetConfiguration) -> pd.DataFrame:
       return await self.storage_strategy.read_data(configuration.input_io_configuration.file_name)
