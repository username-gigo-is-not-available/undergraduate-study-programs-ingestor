import pandas as pd

from src.configurations import StorageConfiguration, DatasetConfiguration
from src.patterns.strategy.storage import LocalStorage, MinioStorage


class FileStorageMixin:
    def __init__(self):
        if StorageConfiguration.FILE_STORAGE_TYPE == 'LOCAL':
            self.file_storage_strategy = LocalStorage()
        elif StorageConfiguration.FILE_STORAGE_TYPE == 'MINIO':
            self.file_storage_strategy = MinioStorage()
        else:
            raise ValueError(f"Unsupported storage type: {StorageConfiguration.FILE_STORAGE_TYPE}")

    async def read_data(self, configuration: DatasetConfiguration) -> pd.DataFrame:
       return await self.file_storage_strategy.read_data(configuration.input_io_configuration.file_name)
