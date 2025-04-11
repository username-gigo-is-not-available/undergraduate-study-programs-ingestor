from pathlib import Path

from src.config import Config
from src.patterns.strategy.processing.base_processing import BaseProcessingStrategy


class StudyProgramProcessingStrategy(BaseProcessingStrategy):
    PATH: Path = Config.STUDY_PROGRAMS_INPUT_DATA_FILE_NAME
    COLUMNS: list[str] = Config.STUDY_PROGRAM_COLUMNS
    COLUMN_MAPPING: dict[str, str] = Config.STUDY_PROGRAM_COLUMN_MAPPING


class CourseProcessingStrategy(BaseProcessingStrategy):
    PATH: Path = Config.COURSES_INPUT_DATA_FILE_NAME
    COLUMNS: list[str] = Config.COURSE_COLUMNS
    COLUMN_MAPPING: dict[str, str] = Config.COURSE_COLUMN_MAPPING


class ProfessorProcessingStrategy(BaseProcessingStrategy):
    PATH: Path = Config.PROFESSORS_INPUT_DATA_FILE_NAME
    COLUMNS: list[str] = Config.PROFESSOR_COLUMNS
    COLUMN_MAPPING: dict[str, str] = Config.PROFESSOR_COLUMN_MAPPING
    PREDICATE: callable = lambda df: df['uid'] != 58
