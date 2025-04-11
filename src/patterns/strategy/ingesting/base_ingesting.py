import logging

import pandas as pd
from neo4j.exceptions import TransientError
from neomodel.async_.core import AsyncDatabase
from neomodel import db
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

from src.config import Config
from src.enums import ComponentType


class BaseIngestingStrategy:
    @classmethod
    async def write(cls, rows: list[dict[str, str | int]], columns: list[str], db: AsyncDatabase, *args, **kwargs) -> None:
        pass

    @classmethod
    async def drop_index(cls, db: AsyncDatabase) -> None:
        pass

    @classmethod
    async def create_index(cls, db: AsyncDatabase) -> None:
        pass

    @classmethod
    @retry(stop=stop_after_attempt(Config.DATABASE_RETRY_COUNT),
           wait=wait_random_exponential(multiplier=Config.DATABASE_RETRY_MULTIPLIER_IN_SECONDS,
                                        exp_base=Config.DATABASE_RETRY_EXPONENT_BASE),
           retry=retry_if_exception_type(TransientError))
    @db.transaction
    async def process(cls, df: pd.DataFrame, db: AsyncDatabase) -> None:
        await cls.write(df.to_dict(orient="records"), df.columns, db)

    @classmethod
    async def run(cls, df: pd.DataFrame, db: AsyncDatabase) -> None:
        await cls.drop_index(db)
        await cls.process(df, db)
        await cls.create_index(db)


class NodeIngestingStrategy(BaseIngestingStrategy):
    COMPONENT_TYPE: ComponentType = ComponentType.NODE
    NODE_LABEL: str = None
    INDEX_COLUMN: str = None

    @classmethod
    async def drop_index(cls, db: AsyncDatabase) -> None:
        cypher: str = f"""
            DROP INDEX {cls.NODE_LABEL.lower()}_{cls.INDEX_COLUMN}_index IF EXISTS
            """
        logging.info(f"Dropping index for {cls.NODE_LABEL} nodes")
        await db.cypher_query(cypher)

    @classmethod
    async def create_index(cls, db: AsyncDatabase) -> None:
        cypher: str = f"""
            CREATE INDEX {cls.NODE_LABEL.lower()}_{cls.INDEX_COLUMN}_index IF NOT EXISTS FOR (n:{cls.NODE_LABEL}) on (n.{cls.INDEX_COLUMN}) 
            """
        logging.info(f"Creating index for {cls.NODE_LABEL} nodes")
        await db.cypher_query(cypher)

    @classmethod
    async def write(cls, rows: list[dict[str, str | int]], columns: list[str], db: AsyncDatabase, *args, **kwargs) -> None:
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

        return await db.cypher_query(cypher, {"rows": rows})


class RelationshipIngestingStrategy(BaseIngestingStrategy):
    COMPONENT_TYPE: ComponentType = ComponentType.RELATIONSHIP
    SOURCE_TARGET_RELATIONSHIP_PROPERTY_NAME: str = None
    TARGET_SOURCE_RELATIONSHIP_PROPERTY_NAME: str = None
    SOURCE_NODE_LABEL: str = None
    TARGET_NODE_LABEL: str = None
    SOURCE_NODE_COLUMN: str = None
    TARGET_NODE_COLUMN: str = None

    @classmethod
    async def write(cls, rows: list[dict[str, str | int]], columns: list[str], db: AsyncDatabase, *args, **kwargs) -> None:
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

        return await db.cypher_query(cypher, {"rows": rows})
