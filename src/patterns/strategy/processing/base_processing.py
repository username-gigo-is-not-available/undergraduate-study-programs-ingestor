from pathlib import Path

import pandas as pd

from src.patterns.mixin.file_storage import FileStorageMixin


class BaseProcessingStrategy():
    COLUMNS: list[str] = []
    COLUMN_MAPPING: dict[str, str] = {}
    PREDICATE: callable = None
    PATH: Path = None

    @classmethod
    async def select(cls, drop_duplicates: bool = True) -> pd.DataFrame:
        df: pd.DataFrame = await FileStorageMixin().read_data(cls.PATH)
        df: pd.DataFrame  = df[cls.COLUMNS].rename(columns=cls.COLUMN_MAPPING)
        return df.drop_duplicates() if drop_duplicates else df

    @classmethod
    async def filter(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df[cls.PREDICATE(df)] if cls.PREDICATE else df

    @classmethod
    async def run(cls) -> pd.DataFrame:
        df: pd.DataFrame = await cls.select()

        if cls.PREDICATE:
            df: pd.DataFrame = await cls.filter(df)

        return df
