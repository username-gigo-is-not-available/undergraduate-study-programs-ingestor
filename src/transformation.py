import pandas as pd


class DataTransformationMixin:

    async def rename(self, df: pd.DataFrame, column_mapping: dict[str, str]) -> pd.DataFrame:
        return df.rename(columns=column_mapping)

    async def cast(self, df: pd.DataFrame, column: str, data_type: type) -> pd.DataFrame:
        df[column] = df[column].astype(data_type)
        return df