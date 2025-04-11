from pathlib import Path

from src.config import Config
from src.enums import CoursePrerequisiteType
from src.patterns.strategy.processing.base_processing import BaseProcessingStrategy


class CurriculumProcessingStrategy(BaseProcessingStrategy):
    PATH: Path = Config.CURRICULA_INPUT_DATA_FILE_NAME
    COLUMNS: list[str] = Config.CURRICULUM_COLUMNS
    COLUMN_MAPPING: dict[str, str] = Config.CURRICULUM_COLUMN_MAPPING


class PrerequisiteProcessingStrategy(BaseProcessingStrategy):
    PATH: Path = Config.PREREQUISITES_INPUT_DATA_FILE_NAME
    COLUMNS: list[str] = Config.PREREQUISITE_COLUMNS
    COLUMN_MAPPING: dict[str, str] = Config.PREREQUISITE_COLUMN_MAPPING
    PREDICATE: callable = lambda df: df['type'] != CoursePrerequisiteType.NONE.value


class TeachesProcessingStrategy(BaseProcessingStrategy):
    PATH: Path = Config.TAUGHT_BY_INPUT_DATA_FILE_NAME
    COLUMNS: list[str] = Config.TEACHES_COLUMNS
    COLUMN_MAPPING: dict[str, str] = Config.TEACHES_COLUMN_MAPPING
    PREDICATE: callable = lambda df: df['professor_id'] != 58
