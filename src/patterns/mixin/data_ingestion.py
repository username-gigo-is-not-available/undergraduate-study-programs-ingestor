import asyncio

import pandas as pd
from neo4j.exceptions import TransientError
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

from src.clients import Neo4jClient
from src.configurations import StorageConfiguration, NodeConfiguration, RelationshipConfiguration


class DataIngestionMixin:

    @retry(stop=stop_after_attempt(StorageConfiguration.DATABASE_RETRY_COUNT),
           wait=wait_random_exponential(
               multiplier=StorageConfiguration.DATABASE_RETRY_MULTIPLIER_IN_SECONDS,
               exp_base=StorageConfiguration.DATABASE_RETRY_EXPONENT_BASE
           ),
           retry=retry_if_exception_type(TransientError)
           )
    async def ingest_nodes(self, df: pd.DataFrame, configuration: NodeConfiguration):
        rows: list[dict[str, str | int]] = df.to_dict(orient='records')

        create_clause: str = f"""
                   CREATE (n:{configuration.label} {{uid: row.uid}})
                   """

        set_clause: str = f"""
                   SET {", ".join([f"n.{column} = row.{column}" for column in configuration.columns()])}
                   """

        cypher: str = f"""
                   UNWIND $rows AS row
                   {create_clause}
                   {set_clause}
                   """
        execute_cypher_task: asyncio.Task = asyncio.create_task(Neo4jClient.execute_cypher(cypher, {"rows": rows}))
        await asyncio.gather(execute_cypher_task)


    @retry(stop=stop_after_attempt(StorageConfiguration.DATABASE_RETRY_COUNT),
           wait=wait_random_exponential(
               multiplier=StorageConfiguration.DATABASE_RETRY_MULTIPLIER_IN_SECONDS,
               exp_base=StorageConfiguration.DATABASE_RETRY_EXPONENT_BASE
           ),
           retry=retry_if_exception_type(TransientError)
           )
    async def ingest_relationships(self, df: list[pd.DataFrame], configuration: RelationshipConfiguration):

        set_columns = set(configuration.columns()) - {
            configuration.source_node.index_column,
            configuration.destination_node.index_column,
        }

        match_clause = f"""
            MATCH (src:{configuration.source_node.label} {{uid: row.{configuration.source_node.index_column}}})
            MATCH (dest:{configuration.destination_node.label} {{uid: row.{configuration.destination_node.index_column}}})
        """

        create_clause = f"""
            CREATE (src)-[r:{configuration.label}]->(dest)
        """

        set_clause = ""
        if set_columns:
            sets = ", ".join([f"r.{col} = row.{col}" for col in set_columns])
            set_clause = f"SET {sets}"

        cypher = f"""
            UNWIND $rows AS row
            {match_clause}
            {create_clause}
            {set_clause}
        """

        execute_cypher_tasks: list[asyncio.Task] = []

        for partition in df:
            rows = partition.to_dict(orient="records")
            execute_cypher_tasks.append(asyncio.create_task(Neo4jClient.execute_cypher(cypher, {"rows": rows})))

        await asyncio.gather(*execute_cypher_tasks)