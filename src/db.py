import logging
from functools import wraps

from neomodel import config
from neomodel.sync_.core import Database

from src.config import Config


def connect_to_neo4j(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        config.DATABASE_URL = Config.DATABASE_CONNECTION_STRING
        config.CONNECTION_ACQUISITION_TIMEOUT = Config.DATABASE_CONNECTION_ACQUISITION_TIMEOUT
        config.CONNECTION_TIMEOUT = Config.DATABASE_CONNECTION_TIMEOUT
        config.MAX_CONNECTION_LIFETIME = Config.DATABASE_MAX_CONNECTION_LIFETIME
        config.MAX_CONNECTION_POOL_SIZE = Config.DATABASE_MAX_CONNECTION_POOL_SIZE
        config.MAX_TRANSACTION_RETRY_TIME = Config.DATABASE_MAX_TRANSACTION_RETRY_TIME

        db = Database()

        try:
            db.cypher_query('RETURN 1')
            logging.info(f"Successfully connected to {Config.DATABASE_NAME} database as user {Config.DATABASE_USER}")
        except Exception as e:
            logging.error(f"Failed to connect to database: {e}")
            return None

        return func(*args, **kwargs)

    return wrapper
