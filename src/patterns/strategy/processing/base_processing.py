import logging
import os
from pathlib import Path
from typing import Hashable

import pandas as pd
from pandas.core.groupby import DataFrameGroupBy

from src.config import Config


class BaseProcessingStrategy:
    COLUMNS: list[str] = []
    COLUMN_MAPPING: dict[str, str] = {}
    PREDICATE: callable = None
    PATH: Path = None

    @classmethod
    async def select(cls, drop_duplicates: bool = True) -> pd.DataFrame:
        df: pd.DataFrame = (pd.read_csv(Config.INPUT_DIRECTORY_PATH / cls.PATH)[cls.COLUMNS]
                            .rename(columns=cls.COLUMN_MAPPING))
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
