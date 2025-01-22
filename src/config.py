import os
from pathlib import Path

from dotenv import dotenv_values
from minio import Minio


class Config:
    ENVIRONMENT_VARIABLES: dict[str, str | int] = {**dotenv_values("../.env"), **os.environ}
    FILE_STORAGE_TYPE: str = ENVIRONMENT_VARIABLES.get("FILE_STORAGE_TYPE")
    MAX_WORKERS: int = os.cpu_count() if ENVIRONMENT_VARIABLES.get('MAX_WORKERS') == 'MAX_WORKERS' else (
        int(ENVIRONMENT_VARIABLES.get('MAX_WORKERS')))

    MINIO_ENDPOINT_URL: str = ENVIRONMENT_VARIABLES.get("MINIO_ENDPOINT_URL")
    MINIO_ACCESS_KEY: str = ENVIRONMENT_VARIABLES.get("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY: str = ENVIRONMENT_VARIABLES.get("MINIO_SECRET_KEY")
    MINIO_SOURCE_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get("MINIO_SOURCE_BUCKET_NAME")
    # MINIO_SECURE_CONNECTION: bool = bool(ENVIRONMENT_VARIABLES.get("MINIO_SECURE_CONNECTION"))
    MINIO_CLIENT = Minio(MINIO_ENDPOINT_URL, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

    INPUT_DIRECTORY_PATH = Path(ENVIRONMENT_VARIABLES.get("INPUT_DIRECTORY_PATH"))
    INPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get("INPUT_FILE_NAME"))

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

    STUDY_PROGRAM_COLUMNS: list[str] = ["study_program_id",
                                        "study_program_code",
                                        "study_program_name",
                                        "study_program_duration",
                                        "study_program_url"]
    COURSE_COLUMNS: list[str] = ["course_id", "course_code", "course_name_mk", "course_name_en", "course_url"]
    PROFESSOR_COLUMNS: list[str] = ["course_professors_id",
                                    "course_professors"]
    CURRICULUM_COLUMNS: list[str] = ["study_program_id", "course_id", "course_level", "course_type", "course_semester",
                                     "course_semester_season",
                                     "course_academic_year"]
    PREREQUISITE_COLUMNS: list[str] = ["course_id", "course_prerequisites_course_id",
                                       "course_prerequisites_minimum_required_courses",
                                       "course_prerequisites",
                                       "course_prerequisite_type"]
    TEACHES_COLUMNS: list[str] = ["course_id", "course_professors_id"]
