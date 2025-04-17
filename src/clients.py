import logging
from typing import Any

from miniopy_async import Minio
from neomodel import config
from neomodel.async_.core import AsyncDatabase
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

from src.config import Config


class Neo4jClient:
    DATABASE_URL: str = Config.DATABASE_CONNECTION_STRING
    DATABASE_CONNECTION_ACQUISITION_TIMEOUT: str = Config.DATABASE_CONNECTION_ACQUISITION_TIMEOUT
    DATABASE_CONNECTION_TIMEOUT: str = Config.DATABASE_CONNECTION_TIMEOUT
    DATABASE_MAX_CONNECTION_LIFETIME: str = Config.DATABASE_MAX_CONNECTION_LIFETIME
    DATABASE_MAX_CONNECTION_POOL_SIZE: int = Config.DATABASE_MAX_CONNECTION_POOL_SIZE
    DATABASE_MAX_TRANSACTION_RETRY_TIME: int = Config.DATABASE_MAX_TRANSACTION_RETRY_TIME
    _instance: 'Neo4jClient' = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            config.DATABASE_URL = cls.DATABASE_URL
            config.CONNECTION_ACQUISITION_TIMEOUT = cls.DATABASE_CONNECTION_ACQUISITION_TIMEOUT
            config.CONNECTION_TIMEOUT = cls.DATABASE_CONNECTION_TIMEOUT
            config.MAX_CONNECTION_LIFETIME = cls.DATABASE_MAX_CONNECTION_LIFETIME
            config.MAX_CONNECTION_POOL_SIZE = cls.DATABASE_MAX_CONNECTION_POOL_SIZE
            config.MAX_TRANSACTION_RETRY_TIME = cls.DATABASE_MAX_TRANSACTION_RETRY_TIME
            cls._instance.client = AsyncDatabase()
        return cls._instance

    @staticmethod
    def connect() -> AsyncDatabase:
        return Neo4jClient().client

    @staticmethod
    @retry(
        stop=stop_after_attempt(Config.DATABASE_RETRY_COUNT),
        wait=wait_random_exponential(
            multiplier=Config.DATABASE_RETRY_MULTIPLIER_IN_SECONDS,
            exp_base=Config.DATABASE_RETRY_EXPONENT_BASE,
        ),
        retry=retry_if_exception_type(),
    )
    async def verify_connection():
        try:
            neo4j: AsyncDatabase = Neo4jClient.connect()
            await neo4j.cypher_query('RETURN 1')
            logging.info(f"Connected to {Config.DATABASE_NAME} database as user {Config.DATABASE_USER}")
        except Exception as e:
            logging.error(f"Connection verification failed: {e}")
            raise

    @staticmethod
    async def disconnect():
        try:
            neo4j: AsyncDatabase = Neo4jClient.connect()
            await neo4j.close_connection()
            logging.info(f"Disconnected from {Config.DATABASE_NAME} database")
        except Exception as e:
            logging.error(f"Disconnection failed: {e}")

    @staticmethod
    async def clear_database():
        try:
            neo4j: AsyncDatabase = Neo4jClient.connect()
            await neo4j.cypher_query('MATCH (n) DETACH DELETE n')
            logging.info(f"Cleared {Config.DATABASE_NAME} database")
        except Exception as e:
            logging.error(f"Clearing database failed: {e}")

    @staticmethod
    async def execute_cypher(cypher: str, params: dict[str, Any]):
        try:
            neo4j: AsyncDatabase = Neo4jClient.connect()
            await neo4j.cypher_query(cypher, params)
            logging.info(f"Executed {cypher} cypher")
        except Exception as e:
            logging.error(f"Execute cypher failed: {e}")


    @staticmethod
    async def create_index(index_name: str, column_name: str, model_name: str):
        try:
            neo4j: AsyncDatabase = Neo4jClient.connect()
            cypher: str = f"""
                CREATE INDEX {index_name} IF NOT EXISTS FOR (n:{model_name}) ON (n:{column_name})
                """
            logging.info(f"Creating index {index_name} on model {model_name} and column {column_name}")
            await neo4j.cypher_query(cypher)
        except Exception as e:
            logging.error(f"Creating index {index_name} on model {model_name} and column {column_name} failed")

    @staticmethod
    async def drop_index(index_name: str):
        try:
            neo4j: AsyncDatabase = Neo4jClient.connect()
            cypher: str = f"""
            DROP INDEX {index_name} IF EXISTS
                """
            logging.info(f"Dropping index  {index_name}")
            await neo4j.cypher_query(cypher)
        except Exception as e:
            logging.error(f"Dropping {index_name} index failed: {e}")


class MinioClient:
    MINIO_ENDPOINT_URL: str = Config.MINIO_ENDPOINT_URL
    MINIO_ACCESS_KEY: str = Config.MINIO_ACCESS_KEY
    MINIO_SECRET_KEY: str = Config.MINIO_SECRET_KEY
    _instance: 'MinioClient' = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance.client = Minio(
                endpoint=cls.MINIO_ENDPOINT_URL,
                access_key=cls.MINIO_ACCESS_KEY,
                secret_key=cls.MINIO_SECRET_KEY,
                secure=False,
            )
        return cls._instance

    @staticmethod
    def connect():
        return MinioClient().client
