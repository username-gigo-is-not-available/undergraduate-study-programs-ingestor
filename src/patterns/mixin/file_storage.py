from pathlib import Path

import pandas as pd

from src.config import Config
from src.patterns.strategy.storage.file_storage import LocalFileStorage, MinioFileStorage


class FileStorageMixin:
    def __init__(self):
        if Config.FILE_STORAGE_TYPE == 'LOCAL':
            self.file_storage_strategy = LocalFileStorage()
        elif Config.FILE_STORAGE_TYPE == 'MINIO':
            self.file_storage_strategy = MinioFileStorage()
        else:
            raise ValueError(f"Unsupported storage type: {Config.FILE_STORAGE_TYPE}")

    def read_data(self, input_file_name: Path) -> pd.DataFrame:
        return self.file_storage_strategy.read_data(input_file_name)
