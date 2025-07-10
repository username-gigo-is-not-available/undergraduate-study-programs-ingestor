import pandas as pd


class DataTransformationMixin:

    async def rename(self, df: pd.DataFrame, column_mapping: dict[str, str]) -> pd.DataFrame:
        return df.rename(columns=column_mapping)
