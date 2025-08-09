import os
from pathlib import Path

from dotenv import dotenv_values

from src.models.enums import DatasetType

ENVIRONMENT_VARIABLES: dict[str, str | int] = {**dotenv_values("../.env"), **os.environ}


class ApplicationConfiguration:
    NUMBER_OF_PARTITIONS: int = 16


class StorageConfiguration:
    FILE_STORAGE_TYPE: str = ENVIRONMENT_VARIABLES.get("FILE_STORAGE_TYPE")

    MINIO_ENDPOINT_URL: str = ENVIRONMENT_VARIABLES.get("MINIO_ENDPOINT_URL")
    MINIO_ACCESS_KEY: str = ENVIRONMENT_VARIABLES.get("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY: str = ENVIRONMENT_VARIABLES.get("MINIO_SECRET_KEY")
    MINIO_INPUT_DATA_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get("MINIO_INPUT_DATA_BUCKET_NAME")
    MINIO_SCHEMA_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get("MINIO_SCHEMA_BUCKET_NAME")
    # MINIO_SECURE_CONNECTION: bool = bool(ENVIRONMENT_VARIABLES.get("MINIO_SECURE_CONNECTION"))

    INPUT_DATA_DIRECTORY_PATH = Path(ENVIRONMENT_VARIABLES.get("INPUT_DATA_DIRECTORY_PATH", ".."))
    SCHEMA_DIRECTORY_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('SCHEMA_DIRECTORY_PATH', '..'))

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


class DatasetIOConfiguration:
    def __init__(self, file_name: str | Path):
        self.file_name = file_name


class PathConfiguration:
    STUDY_PROGRAMS_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("STUDY_PROGRAMS_DATA_INPUT_FILE_NAME"))
    COURSES_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("COURSES_DATA_INPUT_FILE_NAME"))
    PROFESSORS_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("PROFESSORS_DATA_INPUT_FILE_NAME"))
    CURRICULA_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("CURRICULA_DATA_INPUT_FILE_NAME"))
    REQUISITES_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("REQUISITES_DATA_INPUT_FILE_NAME"))
    OFFERS_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("OFFERS_DATA_INPUT_FILE_NAME"))
    INCLUDES_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("INCLUDES_DATA_INPUT_FILE_NAME"))
    REQUIRES_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("REQUIRES_DATA_INPUT_FILE_NAME"))
    SATISFIES_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("SATISFIES_DATA_INPUT_FILE_NAME"))
    TEACHES_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("TEACHES_DATA_INPUT_FILE_NAME"))

    STUDY_PROGRAMS_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("STUDY_PROGRAMS_SCHEMA_FILE_NAME"))
    CURRICULA_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("CURRICULA_SCHEMA_FILE_NAME"))
    COURSES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("COURSES_SCHEMA_FILE_NAME"))
    REQUISITES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("REQUISITES_SCHEMA_FILE_NAME"))
    PROFESSORS_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("PROFESSORS_SCHEMA_FILE_NAME"))
    OFFERS_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("OFFERS_SCHEMA_FILE_NAME"))
    INCLUDES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("INCLUDES_SCHEMA_FILE_NAME"))
    REQUIRES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("REQUIRES_SCHEMA_FILE_NAME"))
    SATISFIES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("SATISFIES_SCHEMA_FILE_NAME"))
    TEACHES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("TEACHES_SCHEMA_FILE_NAME"))


class DatasetConfiguration:
    STUDY_PROGRAMS: "DatasetConfiguration"
    COURSES: "DatasetConfiguration"
    PROFESSORS: "DatasetConfiguration"
    CURRICULA: "DatasetConfiguration"
    REQUISITES: "DatasetConfiguration"
    OFFERS: "DatasetConfiguration"
    INCLUDES: "DatasetConfiguration"
    REQUIRES: "DatasetConfiguration"
    SATISFIES: "DatasetConfiguration"
    TEACHES: "DatasetConfiguration"

    def __init__(self,
                 dataset: DatasetType,
                 input_io_configuration: DatasetIOConfiguration,
                 column_mapping: dict[str, str],
                 schema_configuration: DatasetIOConfiguration,
                 ):
        self.dataset_name = dataset
        self.input_io_configuration = input_io_configuration
        self.column_mapping = column_mapping
        self.schema_configuration = schema_configuration


DatasetConfiguration.STUDY_PROGRAMS = DatasetConfiguration(
    dataset=DatasetType.STUDY_PROGRAMS,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.STUDY_PROGRAMS_INPUT_DATA),
    column_mapping=
    {
        "study_program_id": "uid",
        "study_program_code": "code",
        "study_program_name": "name",
        "study_program_duration": "duration",
        "study_program_url": "url"
    },
    schema_configuration=DatasetIOConfiguration(PathConfiguration.STUDY_PROGRAMS_SCHEMA),
)

DatasetConfiguration.CURRICULA = DatasetConfiguration(
    dataset=DatasetType.CURRICULA,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.CURRICULA_INPUT_DATA),
    column_mapping=
    {
        "curriculum_id": "uid",
        "course_type": "type",
        "course_semester_season": "semester_season",
        "course_academic_year": "academic_year",
        "course_semester": "semester"
    },
    schema_configuration=DatasetIOConfiguration(PathConfiguration.CURRICULA_SCHEMA),
)

DatasetConfiguration.COURSES = DatasetConfiguration(
    dataset=DatasetType.COURSES,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.COURSES_INPUT_DATA),
    column_mapping=
    {
        "course_id": "uid",
        "course_code": "code",
        "course_name_mk": "name_mk",
        "course_name_en": "name_en",
        "course_url": "url",
        "course_level": "level"
    },
    schema_configuration=DatasetIOConfiguration(PathConfiguration.COURSES_SCHEMA),

)

DatasetConfiguration.REQUISITES = DatasetConfiguration(
    dataset=DatasetType.REQUISITES,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.REQUISITES_INPUT_DATA),
    column_mapping=
    {
        "requisite_id": "uid",
        "course_prerequisite_type": "type",
        "minimum_required_number_of_courses": "minimum_required_number_of_courses"
    },
    schema_configuration=DatasetIOConfiguration(PathConfiguration.REQUISITES_SCHEMA),
)

DatasetConfiguration.PROFESSORS = DatasetConfiguration(
    dataset=DatasetType.PROFESSORS,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.PROFESSORS_INPUT_DATA),
    column_mapping=
    {
        "professor_id": "uid",
        "professor_name": "name",
        "professor_surname": "surname"
    },
    schema_configuration=DatasetIOConfiguration(PathConfiguration.PROFESSORS_SCHEMA),
)

DatasetConfiguration.OFFERS = DatasetConfiguration(
    dataset=DatasetType.OFFERS,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.OFFERS_INPUT_DATA),
    column_mapping=
    {
        "offers_id": "uid",
        "curriculum_id": "curriculum_id",
        "study_program_id": "study_program_id"
    },
    schema_configuration=DatasetIOConfiguration(PathConfiguration.OFFERS_SCHEMA),
)

DatasetConfiguration.INCLUDES = DatasetConfiguration(
    dataset=DatasetType.INCLUDES,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.INCLUDES_INPUT_DATA),
    column_mapping=
    {
        "includes_id": "uid",
        "curriculum_id": "curriculum_id",
        "course_id": "course_id"
    },
    schema_configuration=DatasetIOConfiguration(PathConfiguration.INCLUDES_SCHEMA),
)

DatasetConfiguration.REQUIRES = DatasetConfiguration(
    dataset=DatasetType.POSTREQUISITES,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.REQUIRES_INPUT_DATA),
    column_mapping=
    {
        "requires_id": "uid",
        "course_id": "course_id",
        "requisite_id": "requisite_id"
    },
    schema_configuration=DatasetIOConfiguration(PathConfiguration.REQUIRES_SCHEMA),
)

DatasetConfiguration.SATISFIES = DatasetConfiguration(
    dataset=DatasetType.PREREQUISITES,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.SATISFIES_INPUT_DATA),
    column_mapping=
    {
        "satisfies_id": "uid",
        "prerequisite_course_id": "prerequisite_course_id",
        "requisite_id": "requisite_id"
    },
    schema_configuration=DatasetIOConfiguration(PathConfiguration.SATISFIES_SCHEMA),
)

DatasetConfiguration.TEACHES = DatasetConfiguration(
    dataset=DatasetType.TEACHES,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.TEACHES_INPUT_DATA),
    column_mapping=
    {
        "teaches_id": "uid",
        "course_id": "course_id",
        "professor_id": "professor_id"
    },
    schema_configuration=DatasetIOConfiguration(PathConfiguration.TEACHES_SCHEMA),
)


class PartitioningConfiguration:
    OFFERS: "PartitioningConfiguration"
    INCLUDES: "PartitioningConfiguration"
    PREREQUISITES: "PartitioningConfiguration"
    POSTREQUISITES: "PartitioningConfiguration"
    TEACHES: "PartitioningConfiguration"

    def __init__(self,
                 source_node_column: str,
                 destination_node_column: str):
        self.source_node_column = source_node_column
        self.destination_node_column = destination_node_column


PartitioningConfiguration.OFFERS = PartitioningConfiguration(
    source_node_column="study_program_id",
    destination_node_column="curriculum_id",
)

PartitioningConfiguration.INCLUDES = PartitioningConfiguration(
    source_node_column="curriculum_id",
    destination_node_column="course_id",
)

PartitioningConfiguration.PREREQUISITES = PartitioningConfiguration(
    source_node_column="prerequisite_course_id",
    destination_node_column="requisite_id",
)

PartitioningConfiguration.POSTREQUISITES = PartitioningConfiguration(
    source_node_column="course_id",
    destination_node_column="requisite_id",
)

PartitioningConfiguration.TEACHES = PartitioningConfiguration(
    source_node_column="professor_id",
    destination_node_column="course_id",
)


class NodeIngestionConfiguration:
    STUDY_PROGRAMS: "NodeIngestionConfiguration"
    COURSES: "NodeIngestionConfiguration"
    PROFESSORS: "NodeIngestionConfiguration"
    CURRICULA: "NodeIngestionConfiguration"
    REQUISITES: "NodeIngestionConfiguration"

    def __init__(self, label: str, columns: list[str], index_column="uid"):
        self.label = label
        self.columns = columns
        self.index_column = index_column


NodeIngestionConfiguration.STUDY_PROGRAMS = NodeIngestionConfiguration(
    label="StudyProgram",
    columns=list(DatasetConfiguration.STUDY_PROGRAMS.column_mapping.values())
)

NodeIngestionConfiguration.COURSES = NodeIngestionConfiguration(
    label="Course",
    columns=list(DatasetConfiguration.COURSES.column_mapping.values())
)

NodeIngestionConfiguration.PROFESSORS = NodeIngestionConfiguration(
    label="Professor",
    columns=list(DatasetConfiguration.PROFESSORS.column_mapping.values())
)

NodeIngestionConfiguration.CURRICULA = NodeIngestionConfiguration(
    label="Curriculum",
    columns=list(DatasetConfiguration.CURRICULA.column_mapping.values())
)

NodeIngestionConfiguration.REQUISITES = NodeIngestionConfiguration(
    label="Requisite",
    columns=list(DatasetConfiguration.REQUISITES.column_mapping.values())
)


class RelationshipIngestionConfiguration:
    OFFERS: "RelationshipIngestionConfiguration"
    INCLUDES: "RelationshipIngestionConfiguration"
    REQUIRES: "RelationshipIngestionConfiguration"
    SATISFIES: "RelationshipIngestionConfiguration"
    TEACHES: "RelationshipIngestionConfiguration"

    def __init__(self,
                 source_node_label: str,
                 destination_node_label: str,
                 source_node_column: str,
                 destination_node_column: str,
                 label: str,
                 columns: list[str],
                 index_column: str = "uid"
                 ):
        self.columns = columns
        self.source_node_label = source_node_label
        self.destination_node_label = destination_node_label
        self.source_node_column = source_node_column
        self.destination_node_column = destination_node_column
        self.label = label
        self.index_column = index_column


RelationshipIngestionConfiguration.OFFERS = RelationshipIngestionConfiguration(
    source_node_label="StudyProgram",
    destination_node_label="Curriculum",
    source_node_column=PartitioningConfiguration.OFFERS.source_node_column,
    destination_node_column=PartitioningConfiguration.OFFERS.destination_node_column,
    label="OFFERS",
    columns=list(DatasetConfiguration.OFFERS.column_mapping.values())
)

RelationshipIngestionConfiguration.INCLUDES = RelationshipIngestionConfiguration(
    source_node_label="Curriculum",
    destination_node_label="Course",
    source_node_column=PartitioningConfiguration.INCLUDES.source_node_column,
    destination_node_column=PartitioningConfiguration.INCLUDES.destination_node_column,
    label="INCLUDES",
    columns=list(DatasetConfiguration.INCLUDES.column_mapping.values())
)

RelationshipIngestionConfiguration.REQUIRES = RelationshipIngestionConfiguration(
    source_node_label="Course",
    destination_node_label="Requisite",
    source_node_column=PartitioningConfiguration.POSTREQUISITES.source_node_column,
    destination_node_column=PartitioningConfiguration.POSTREQUISITES.destination_node_column,
    label="REQUIRES",
    columns=list(DatasetConfiguration.REQUIRES.column_mapping.values())
)

RelationshipIngestionConfiguration.SATISFIES = RelationshipIngestionConfiguration(
    source_node_label="Course",
    destination_node_label="Requisite",
    source_node_column=PartitioningConfiguration.PREREQUISITES.source_node_column,
    destination_node_column=PartitioningConfiguration.PREREQUISITES.destination_node_column,
    label="SATISFIES",
    columns=list(DatasetConfiguration.SATISFIES.column_mapping.values())
)

RelationshipIngestionConfiguration.TEACHES = RelationshipIngestionConfiguration(
    source_node_label="Professor",
    destination_node_label="Course",
    source_node_column=PartitioningConfiguration.TEACHES.source_node_column,
    destination_node_column=PartitioningConfiguration.TEACHES.destination_node_column,
    label="TEACHES",
    columns=list(DatasetConfiguration.TEACHES.column_mapping.values())
)
