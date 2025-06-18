from pathlib import Path

from src.config import Config
from src.patterns.strategy.processing.base_processing import BaseProcessingStrategy


class OffersProcessingStrategy(BaseProcessingStrategy):
    PATH: Path = Config.OFFERS_INPUT_DATA_FILE_NAME
    COLUMNS: list[str] = Config.OFFERS_COLUMNS
    COLUMN_MAPPING: dict[str, str] = Config.OFFERS_COLUMN_MAPPING


class RequiresProcessingStrategy(BaseProcessingStrategy):
    PATH: Path = Config.REQUIRES_INPUT_DATA_FILE_NAME
    COLUMNS: list[str] = Config.REQUIRES_COLUMNS
    COLUMN_MAPPING: dict[str, str] = Config.REQUIRES_COLUMN_MAPPING


class TeachesProcessingStrategy(BaseProcessingStrategy):
    PATH: Path = Config.TEACHES_INPUT_DATA_FILE_NAME
    COLUMNS: list[str] = Config.TEACHES_COLUMNS
    COLUMN_MAPPING: dict[str, str] = Config.TEACHES_COLUMN_MAPPING
