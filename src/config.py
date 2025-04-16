import os
from pathlib import Path

from dotenv import dotenv_values


class Config:
    ENVIRONMENT_VARIABLES: dict[str, str | int] = {**dotenv_values("../.env"), **os.environ}
    FILE_STORAGE_TYPE: str = ENVIRONMENT_VARIABLES.get("FILE_STORAGE_TYPE")

    MINIO_ENDPOINT_URL: str = ENVIRONMENT_VARIABLES.get("MINIO_ENDPOINT_URL")
    MINIO_ACCESS_KEY: str = ENVIRONMENT_VARIABLES.get("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY: str = ENVIRONMENT_VARIABLES.get("MINIO_SECRET_KEY")
    MINIO_SOURCE_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get("MINIO_SOURCE_BUCKET_NAME")
    # MINIO_SECURE_CONNECTION: bool = bool(ENVIRONMENT_VARIABLES.get("MINIO_SECURE_CONNECTION"))

    INPUT_DIRECTORY_PATH = Path(ENVIRONMENT_VARIABLES.get("INPUT_DIRECTORY_PATH"))
    STUDY_PROGRAMS_INPUT_DATA_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get("STUDY_PROGRAMS_INPUT_DATA_FILE_NAME"))
    COURSES_INPUT_DATA_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('COURSES_INPUT_DATA_FILE_NAME'))
    PROFESSORS_INPUT_DATA_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('PROFESSORS_INPUT_DATA_FILE_NAME'))
    CURRICULA_INPUT_DATA_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('CURRICULA_INPUT_DATA_FILE_NAME'))
    TAUGHT_BY_INPUT_DATA_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('TAUGHT_BY_INPUT_DATA_FILE_NAME'))
    PREREQUISITES_INPUT_DATA_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('PREREQUISITES_INPUT_DATA_FILE_NAME'))

    DATABASE_USER: str = ENVIRONMENT_VARIABLES.get("DATABASE_USER")
    DATABASE_PASSWORD: str = ENVIRONMENT_VARIABLES.get("DATABASE_PASSWORD")
    DATABASE_HOST_NAME: str = ENVIRONMENT_VARIABLES.get("DATABASE_HOST_NAME")
    DATABASE_PORT: int = ENVIRONMENT_VARIABLES.get("DATABASE_PORT")
    DATABASE_NAME: str = ENVIRONMENT_VARIABLES.get("DATABASE_NAME")
    DATABASE_CONNECTION_STRING: str = fr"neo4j://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST_NAME}:{DATABASE_PORT}/{DATABASE_NAME}"
    DATABASE_CONNECTION_ACQUISITION_TIMEOUT: float = float(ENVIRONMENT_VARIABLES.get("DATABASE_CONNECTION_ACQUISITION_TIMEOUT"))
    DATABASE_CONNECTION_TIMEOUT: float = float(ENVIRONMENT_VARIABLES.get("DATABASE_CONNECTION_TIMEOUT"))
    DATABASE_MAX_CONNECTION_LIFETIME: int = int(ENVIRONMENT_VARIABLES.get("DATABASE_MAX_CONNECTION_LIFETIME"))
    DATABASE_MAX_CONNECTION_POOL_SIZE: int = int(ENVIRONMENT_VARIABLES.get("DATABASE_MAX_CONNECTION_POOL_SIZE"))
    DATABASE_MAX_TRANSACTION_RETRY_TIME: int = int(ENVIRONMENT_VARIABLES.get("DATABASE_MAX_TRANSACTION_RETRY_TIME"))

    DATABASE_RETRY_COUNT: int = int(ENVIRONMENT_VARIABLES.get("DATABASE_RETRY_COUNT"))
    DATABASE_RETRY_MULTIPLIER_IN_SECONDS: int = int(ENVIRONMENT_VARIABLES.get("DATABASE_RETRY_MULTIPLIER_IN_SECONDS"))
    DATABASE_RETRY_EXPONENT_BASE: int = int(ENVIRONMENT_VARIABLES.get("DATABASE_RETRY_EXPONENT_BASE"))

    STUDY_PROGRAM_COLUMNS: list[str] = [
        "study_program_id",
        "study_program_code",
        "study_program_name",
        "study_program_duration",
        "study_program_url"
    ]
    COURSE_COLUMNS: list[str] = [
        "course_id",
        "course_code",
        "course_name_mk",
        "course_name_en",
        "course_url"
    ]
    PROFESSOR_COLUMNS: list[str] = [
        'professor_id',
        'professor_name',
        'professor_surname'
    ]
    CURRICULUM_COLUMNS: list[str] = [
        'study_program_id',
        'course_id',
        'course_type',
        'course_semester',
        'course_semester_season',
        'course_academic_year',
        'course_level',
    ]
    PREREQUISITE_COLUMNS: list[str] = [
        'course_id',
        'course_prerequisite_type',
        'course_prerequisite_id',
        'minimum_required_number_of_courses'
    ]
    TEACHES_COLUMNS: list[str] = [
        "course_id",
        "professor_id"
    ]

    STUDY_PROGRAM_COLUMN_MAPPING: dict[str, str] = {
        "study_program_id": "uid",
        "study_program_code": "code",
        "study_program_name": "name",
        "study_program_duration": "duration",
        "study_program_url": "url"
    }
    COURSE_COLUMN_MAPPING: dict[str, str] = {
        "course_id": "uid",
        "course_code": "code",
        "course_name_mk": "name_mk",
        "course_name_en": "name_en",
        "course_url": "url"
    }
    PROFESSOR_COLUMN_MAPPING: dict[str, str] = {
        "professor_id": "uid",
        "professor_name": "name",
        "professor_surname": "surname"
    }
    CURRICULUM_COLUMN_MAPPING: dict[str, str] = {
        'study_program_id': 'study_program_id',
        'course_id': 'course_id',
        'course_level': 'level',
        'course_type': 'type',
        'course_semester': 'semester',
        'course_semester_season': 'semester_season',
        'course_academic_year': 'academic_year',
    }
    PREREQUISITE_COLUMN_MAPPING: dict[str, str] = {
        'course_id': 'course_id',
        'course_prerequisite_id': 'course_prerequisite_id',
        'course_prerequisite_type': 'type',
        'minimum_required_number_of_courses': 'number_of_courses'
    }
    TEACHES_COLUMN_MAPPING: dict[str, str] = {
        "course_id": "course_id",
        "professor_id": "professor_id"
    }
