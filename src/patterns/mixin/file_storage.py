import pandas as pd

from src.configurations import StorageConfiguration, DatasetConfiguration
from src.patterns.strategy.file_storage import LocalFileStorage, MinioFileStorage


class FileStorageMixin:
    def __init__(self):
        if StorageConfiguration.FILE_STORAGE_TYPE == 'LOCAL':
            self.file_storage_strategy = LocalFileStorage()
        elif StorageConfiguration.FILE_STORAGE_TYPE == 'MINIO':
            self.file_storage_strategy = MinioFileStorage()
        else:
            raise ValueError(f"Unsupported storage type: {StorageConfiguration.FILE_STORAGE_TYPE}")

    async def read_data(self, configuration: DatasetConfiguration) -> pd.DataFrame:
       return await self.file_storage_strategy.read_data(configuration.input_io_config.file_name)
