import re

from src.setup import ENVIRONMENT_VARIABLES
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.models.enums import FileIOType

@dataclass(frozen=True)
class NodeConfiguration:

    dataset_name: str
    column_mapping: dict[str, Any]
    label: str
    index_column: str = "uid"

    def input_columns(self) -> list[str]:
        return list(self.column_mapping.keys())

    def output_columns(self) -> list[str]:
        return list(self.column_mapping.values())

    def labeled_index_column(self):
        column: str = "_".join(re.findall(r'[A-Z][a-z]*', self.label))
        return "_".join([column.lower(), "id"])

@dataclass(frozen=True)
class RelationshipConfiguration:

    dataset_name: str
    column_mapping: dict[str, Any]
    label: str
    source_node: NodeConfiguration
    destination_node: NodeConfiguration
    index_column: str = "uid"

    def input_columns(self) -> list[str]:
        return list(self.column_mapping.keys())

    def output_columns(self) -> list[str]:
        return list(self.column_mapping.values())

STUDY_PROGRAMS: "NodeConfiguration" = NodeConfiguration(
        dataset_name=ENVIRONMENT_VARIABLES.get("STUDY_PROGRAMS_DATASET_NAME"),
        column_mapping=
        {
            "study_program_id": "uid",
            "study_program_code": "code",
            "study_program_name": "name",
            "study_program_duration": "duration",
            "study_program_url": "url"
        },
        label="StudyProgram"
    )

CURRICULA: NodeConfiguration = NodeConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get("CURRICULA_DATASET_NAME"),
    column_mapping=
    {
        "curriculum_id": "uid",
        "course_type": "type",
        "course_semester_season": "semester_season",
        "course_semester": "semester",
        "course_academic_year": "academic_year"
    },
    label="Curriculum"
)

COURSES: NodeConfiguration = NodeConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get("COURSES_DATASET_NAME"),
    column_mapping=
    {
        "course_id": "uid",
        "course_code": "code",
        "course_name_mk": "name_mk",
        "course_name_en": "name_en",
        "course_abbreviation": "abbreviation",
        "course_url": "url",
        "course_level": "level"
    },
    label="Course"
)

REQUISITES: NodeConfiguration = NodeConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get("REQUISITES_DATASET_NAME"),
    column_mapping=
    {
        "requisite_id": "uid",
        "course_prerequisite_type": "type",
        "minimum_required_number_of_courses": "minimum_required_number_of_courses"
    },
    label="Requisite"
)

PROFESSORS: NodeConfiguration = NodeConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('PROFESSORS_DATASET_NAME'),
    column_mapping=
    {
        "professor_id": "uid",
        "professor_name": "name",
        "professor_surname": "surname"
    },
    label="Professor"
)

OFFERS: RelationshipConfiguration = RelationshipConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('OFFERS_DATASET_NAME'),
    column_mapping=
    {
        "offers_id": "uid",
        "curriculum_id": "curriculum_id",
        "study_program_id": "study_program_id"
    },
    label="OFFERS",
    source_node=STUDY_PROGRAMS,
    destination_node=CURRICULA
)

INCLUDES: RelationshipConfiguration = RelationshipConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('INCLUDES_DATASET_NAME'),
    column_mapping=
    {
        "includes_id": "uid",
        "curriculum_id": "curriculum_id",
        "course_id": "course_id"
    },
    label="INCLUDES",
    source_node=CURRICULA,
    destination_node=COURSES
)

REQUIRES: RelationshipConfiguration = RelationshipConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('REQUIRES_DATASET_NAME'),
    column_mapping=
    {
        "requires_id": "uid",
        "course_id": "course_id",
        "requisite_id": "requisite_id"
    },
    label="REQUIRES",
    source_node=COURSES,
    destination_node=REQUISITES
)

SATISFIES: RelationshipConfiguration = RelationshipConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('SATISFIES_DATASET_NAME'),
    column_mapping=
    {
        "satisfies_id": "uid",
        "course_id": "course_id",
        "requisite_id": "requisite_id"
    },
    label="SATISFIES",
    source_node=COURSES,
    destination_node=REQUISITES
)

TEACHES: RelationshipConfiguration = RelationshipConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('TEACHES_DATASET_NAME'),
    column_mapping=
    {
        "teaches_id": "uid",
        "course_id": "course_id",
        "professor_id": "professor_id"
    },
    label="TEACHES",
    source_node=PROFESSORS,
    destination_node=COURSES
)

class ApplicationConfiguration:
    NUMBER_OF_PARTITIONS: int = 16


class StorageConfiguration:
    FILE_IO_TYPE: FileIOType = FileIOType(ENVIRONMENT_VARIABLES.get('FILE_IO_TYPE').upper())
    LOCAL_ICEBERG_LAKEHOUSE_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('LOCAL_ICEBERG_LAKEHOUSE_FILE_PATH'))
    S3_ENDPOINT_URL: str = ENVIRONMENT_VARIABLES.get('S3_ENDPOINT_URL')
    S3_ACCESS_KEY: str = ENVIRONMENT_VARIABLES.get('S3_ACCESS_KEY')
    S3_SECRET_KEY: str = ENVIRONMENT_VARIABLES.get('S3_SECRET_KEY')
    S3_ICEBERG_LAKEHOUSE_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get('S3_ICEBERG_LAKEHOUSE_BUCKET_NAME')
    S3_PATH_STYLE_ACCESS: bool = ENVIRONMENT_VARIABLES.get('S3_PATH_STYLE_ACCESS')
    ICEBERG_CATALOG_NAME: str = ENVIRONMENT_VARIABLES.get("ICEBERG_CATALOG_NAME")
    ICEBERG_NAMESPACE: str = ENVIRONMENT_VARIABLES.get("ICEBERG_NAMESPACE")

    DATABASE_USER: str = ENVIRONMENT_VARIABLES.get("DATABASE_USER")
    DATABASE_PASSWORD: str = ENVIRONMENT_VARIABLES.get("DATABASE_PASSWORD")
    DATABASE_HOST_NAME: str = ENVIRONMENT_VARIABLES.get("DATABASE_HOST_NAME")
    DATABASE_PORT: int = ENVIRONMENT_VARIABLES.get("DATABASE_PORT")
    DATABASE_NAME: str = ENVIRONMENT_VARIABLES.get("DATABASE_NAME")
    DATABASE_CONNECTION_STRING: str = fr"neo4j://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST_NAME}:{DATABASE_PORT}/{DATABASE_NAME}"
    DATABASE_CONNECTION_ACQUISITION_TIMEOUT: float = float(
        ENVIRONMENT_VARIABLES.get("DATABASE_CONNECTION_ACQUISITION_TIMEOUT"))
    DATABASE_CONNECTION_TIMEOUT: float = float(ENVIRONMENT_VARIABLES.get("DATABASE_CONNECTION_TIMEOUT"))
    DATABASE_MAX_CONNECTION_LIFETIME: int = int(ENVIRONMENT_VARIABLES.get("DATABASE_MAX_CONNECTION_LIFETIME"))
    DATABASE_MAX_CONNECTION_POOL_SIZE: int = int(ENVIRONMENT_VARIABLES.get("DATABASE_MAX_CONNECTION_POOL_SIZE"))
    DATABASE_MAX_TRANSACTION_RETRY_TIME: int = int(ENVIRONMENT_VARIABLES.get("DATABASE_MAX_TRANSACTION_RETRY_TIME"))

    DATABASE_RETRY_COUNT: int = int(ENVIRONMENT_VARIABLES.get("DATABASE_RETRY_COUNT"))
    DATABASE_RETRY_MULTIPLIER_IN_SECONDS: int = int(ENVIRONMENT_VARIABLES.get("DATABASE_RETRY_MULTIPLIER_IN_SECONDS"))
    DATABASE_RETRY_EXPONENT_BASE: int = int(ENVIRONMENT_VARIABLES.get("DATABASE_RETRY_EXPONENT_BASE"))



