import logging

import pandas as pd
from neo4j.exceptions import TransientError
from neomodel import db
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

from src.clients import Neo4jClient
from src.config import Config
from src.models.enums import ComponentType


class BaseIngestingStrategy:
    @classmethod
    async def write(cls, rows: list[dict[str, str | int]], columns: list[str], *args, **kwargs) -> None:
        pass

    @classmethod
    async def drop_index(cls) -> None:
        pass

    @classmethod
    async def create_index(cls) -> None:
        pass

    @classmethod
    @retry(stop=stop_after_attempt(Config.DATABASE_RETRY_COUNT),
           wait=wait_random_exponential(multiplier=Config.DATABASE_RETRY_MULTIPLIER_IN_SECONDS,
                                        exp_base=Config.DATABASE_RETRY_EXPONENT_BASE),
           retry=retry_if_exception_type(TransientError))
    @db.transaction
    async def process(cls, df: pd.DataFrame) -> None:
        await cls.write(df.to_dict(orient="records"), df.columns)

    @classmethod
    async def run(cls, df: pd.DataFrame) -> None:
        await cls.process(df)


class NodeIngestingStrategy(BaseIngestingStrategy):
    COMPONENT_TYPE: ComponentType = ComponentType.NODE
    NODE_LABEL: str = None
    INDEX_COLUMN: str = None


    @classmethod
    async def write(cls, rows: list[dict[str, str | int]], columns: list[str], *args, **kwargs) -> None:
        logging.info(f"Created nodes: {rows}")

        create_clause: str = f"""
            CREATE (n:{cls.NODE_LABEL} {{uid: row.uid}})
            """

        set_clause: str = f"""
            SET {", ".join([f"n.{column} = row.{column}" for column in columns])}
            """

        cypher: str = f"""
            UNWIND $rows AS row
            {create_clause}
            {set_clause}
            """
        await Neo4jClient.drop_index(index_name=f"{cls.NODE_LABEL.lower()}_{cls.INDEX_COLUMN}_index")
        await Neo4jClient.execute_cypher(cypher, {"rows": rows})
        await Neo4jClient.create_index(index_name=f"{cls.NODE_LABEL.lower()}_{cls.INDEX_COLUMN}_index",
                                       column_name=cls.INDEX_COLUMN, model_name=cls.NODE_LABEL)


class RelationshipIngestingStrategy(BaseIngestingStrategy):
    COMPONENT_TYPE: ComponentType = ComponentType.RELATIONSHIP
    SOURCE_TARGET_RELATIONSHIP_PROPERTY_NAME: str = None
    TARGET_SOURCE_RELATIONSHIP_PROPERTY_NAME: str = None
    SOURCE_NODE_LABEL: str = None
    TARGET_NODE_LABEL: str = None
    SOURCE_NODE_COLUMN: str = None
    TARGET_NODE_COLUMN: str = None

    @classmethod
    async def write(cls, rows: list[dict[str, str | int]], columns: list[str], *args, **kwargs) -> None:
        logging.info(f"Created relationships: {rows}")

        set_columns = set(columns) - {cls.SOURCE_NODE_COLUMN, cls.TARGET_NODE_COLUMN}
        match_clause = f"""
            MATCH (src:{cls.SOURCE_NODE_LABEL} {{uid: row.{cls.SOURCE_NODE_COLUMN}}})
            MATCH (dest:{cls.TARGET_NODE_LABEL} {{uid: row.{cls.TARGET_NODE_COLUMN}}})
            """

        create_clause = f"""
            CREATE (src)-[r1:{cls.SOURCE_TARGET_RELATIONSHIP_PROPERTY_NAME}]->(dest)
            CREATE (dest)-[r2:{cls.TARGET_SOURCE_RELATIONSHIP_PROPERTY_NAME}]->(src)
            """

        set_clause = ""
        if set_columns:
            r1_sets = ", ".join([f"r1.{col} = row.{col}" for col in set_columns])
            r2_sets = ", ".join([f"r2.{col} = row.{col}" for col in set_columns])
            set_clause = f"SET {r1_sets}, {r2_sets}"

        cypher = f"""
            UNWIND $rows AS row
            {match_clause}
            {create_clause}
            {set_clause}
            """
        await Neo4jClient.execute_cypher(cypher, {"rows": rows})
