import logging

from neomodel import config
from neomodel.async_.core import AsyncDatabase
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
from neomodel.exceptions import NotConnected, DoesNotExist
from src.config import Config


class DatabaseMixin:
    config.DATABASE_URL = Config.DATABASE_CONNECTION_STRING
    config.CONNECTION_ACQUISITION_TIMEOUT = Config.DATABASE_CONNECTION_ACQUISITION_TIMEOUT
    config.CONNECTION_TIMEOUT = Config.DATABASE_CONNECTION_TIMEOUT
    config.MAX_CONNECTION_LIFETIME = Config.DATABASE_MAX_CONNECTION_LIFETIME
    config.MAX_CONNECTION_POOL_SIZE = Config.DATABASE_MAX_CONNECTION_POOL_SIZE
    config.MAX_TRANSACTION_RETRY_TIME = Config.DATABASE_MAX_TRANSACTION_RETRY_TIME

    @staticmethod
    @retry(stop=stop_after_attempt(Config.DATABASE_RETRY_COUNT),
           wait=wait_random_exponential(multiplier=Config.DATABASE_RETRY_MULTIPLIER_IN_SECONDS,
                                        exp_base=Config.DATABASE_RETRY_EXPONENT_BASE),
           retry=retry_if_exception_type(NotConnected)
           )
    async def connect():
        db = AsyncDatabase()

        try:
            await db.cypher_query('RETURN 1')
            logging.info(f"Successfully connected to {Config.DATABASE_NAME} database as user {Config.DATABASE_USER}")
        except Exception as e:
            logging.error(f"Failed to connect to database: {e}")
            return None

        return db

    @staticmethod
    @retry(stop=stop_after_attempt(Config.DATABASE_RETRY_COUNT),
           wait=wait_random_exponential(multiplier=Config.DATABASE_RETRY_MULTIPLIER_IN_SECONDS,
                                        exp_base=Config.DATABASE_RETRY_EXPONENT_BASE),
           retry=retry_if_exception_type(DoesNotExist)
           )
    async def disconnect(db: AsyncDatabase):
        try:
            await db.close_connection()
            logging.info(f"Successfully disconnected from {Config.DATABASE_NAME} database")
        except Exception as e:
            logging.error(f"Failed to disconnect from database: {e}")

    @staticmethod
    @retry(stop=stop_after_attempt(Config.DATABASE_RETRY_COUNT),
           wait=wait_random_exponential(multiplier=Config.DATABASE_RETRY_MULTIPLIER_IN_SECONDS,
                                        exp_base=Config.DATABASE_RETRY_EXPONENT_BASE),
           retry=retry_if_exception_type(NotConnected)
           )
    async def clear_database(db: AsyncDatabase):
        try:
            await db.cypher_query('MATCH (n) DETACH DELETE n')
            logging.info(f"Successfully cleared {Config.DATABASE_NAME} database")
        except Exception as e:
            logging.error(f"Failed to clear database: {e}")


