import pandas as pd

from src.configurations import PartitioningConfiguration


class DataPartitionMixin:

    async def generate_partition_uid(self, df: pd.DataFrame,
                                     configuration: PartitioningConfiguration) -> pd.DataFrame:

        df["partition_uid"] = df.apply(
            lambda row: "-".join([
                row[configuration.source_node_column][-1],
                row[configuration.destination_node_column][-1]
            ]),
            axis=1
        )

        return df

    async def filter_by_partition(self, df: pd.DataFrame, partition_set: list[str]) -> pd.DataFrame:
        return df[df["partition_uid"].isin(partition_set)]

    async def generate_partitions_and_batches(self, partition_size: int) -> list[list[str]]:
        batches: list[list[str]] = []
        for i in range(partition_size):
            partitions: list[str] = []
            for j in range(partition_size):
                k: int = (i + j) % partition_size
                partitions.append(str(k) + '-' + str(j))
            batches.append(partitions)
        return batches

    async def partition(self, df: pd.DataFrame) -> list[pd.DataFrame]:

        batches: list[list[str]] = await self.generate_partitions_and_batches(16)

        partitions: list[pd.DataFrame] = []
        for index, partition_set in enumerate(batches):
            batch: pd.DataFrame = await self.filter_by_partition(df, partition_set)
            partitions.append(batch)

        return partitions
