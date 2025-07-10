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
    MINIO_SOURCE_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get("MINIO_SOURCE_BUCKET_NAME")
    # MINIO_SECURE_CONNECTION: bool = bool(ENVIRONMENT_VARIABLES.get("MINIO_SECURE_CONNECTION"))

    INPUT_DIRECTORY_PATH = Path(ENVIRONMENT_VARIABLES.get("INPUT_DIRECTORY_PATH"))

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


class DatasetPathConfiguration:
    STUDY_PROGRAMS_INPUT: Path = Path(ENVIRONMENT_VARIABLES.get("STUDY_PROGRAMS_DATA_INPUT_FILE_NAME"))

    COURSES_INPUT: Path = Path(ENVIRONMENT_VARIABLES.get("COURSES_DATA_INPUT_FILE_NAME"))

    PROFESSORS_INPUT: Path = Path(ENVIRONMENT_VARIABLES.get("PROFESSORS_DATA_INPUT_FILE_NAME"))

    CURRICULA_INPUT: Path = Path(ENVIRONMENT_VARIABLES.get("CURRICULA_DATA_INPUT_FILE_NAME"))

    REQUISITES_INPUT: Path = Path(ENVIRONMENT_VARIABLES.get("REQUISITES_DATA_INPUT_FILE_NAME"))

    OFFERS_INPUT: Path = Path(ENVIRONMENT_VARIABLES.get("OFFERS_DATA_INPUT_FILE_NAME"))

    INCLUDES_INPUT: Path = Path(ENVIRONMENT_VARIABLES.get("INCLUDES_DATA_INPUT_FILE_NAME"))

    PREREQUISITES_INPUT: Path = Path(ENVIRONMENT_VARIABLES.get("PREREQUISITES_DATA_INPUT_FILE_NAME"))

    POSTREQUISITES_INPUT: Path = Path(ENVIRONMENT_VARIABLES.get("POSTREQUISITES_DATA_INPUT_FILE_NAME"))

    TEACHES_INPUT: Path = Path(ENVIRONMENT_VARIABLES.get("TEACHES_DATA_INPUT_FILE_NAME"))


class DatasetTransformationConfiguration:
    def __init__(self, columns: list[str], column_mapping: dict[str, str]):
        self.columns = columns
        self.column_mapping = column_mapping


class DatasetConfiguration:
    STUDY_PROGRAMS: "DatasetConfiguration"
    COURSES: "DatasetConfiguration"
    PROFESSORS: "DatasetConfiguration"
    CURRICULA: "DatasetConfiguration"
    REQUISITES: "DatasetConfiguration"
    OFFERS: "DatasetConfiguration"
    INCLUDES: "DatasetConfiguration"
    PREREQUISITES: "DatasetConfiguration"
    POSTREQUISITES: "DatasetConfiguration"
    TEACHES: "DatasetConfiguration"

    def __init__(self,
                 dataset: DatasetType,
                 input_io_config: DatasetIOConfiguration,
                 transformation_config: DatasetTransformationConfiguration,
                 ):
        self.dataset_name = dataset
        self.input_io_config = input_io_config
        self.transformation_config = transformation_config


DatasetConfiguration.STUDY_PROGRAMS = DatasetConfiguration(
    dataset=DatasetType.STUDY_PROGRAMS,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.STUDY_PROGRAMS_INPUT),
    transformation_config=DatasetTransformationConfiguration(
        columns=[
            "study_program_id",
            "study_program_code",
            "study_program_name",
            "study_program_duration",
            "study_program_url"
        ],
        column_mapping=
        {
            "study_program_id": "uid",
            "study_program_code": "code",
            "study_program_name": "name",
            "study_program_duration": "duration",
            "study_program_url": "url"
        }
    )
)

DatasetConfiguration.COURSES = DatasetConfiguration(
    dataset=DatasetType.COURSES,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.COURSES_INPUT),
    transformation_config=DatasetTransformationConfiguration(
        columns=
        [
            "course_id",
            "course_code",
            "course_name_mk",
            "course_name_en",
            "course_url",
            "course_level"
        ],
        column_mapping=
        {
            "course_id": "uid",
            "course_code": "code",
            "course_name_mk": "name_mk",
            "course_name_en": "name_en",
            "course_url": "url",
            "course_level": "level"
        }
    )
)

DatasetConfiguration.PROFESSORS = DatasetConfiguration(
    dataset=DatasetType.PROFESSORS,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.PROFESSORS_INPUT),
    transformation_config=DatasetTransformationConfiguration(
        columns=
        [
            "professor_id",
            "professor_name",
            "professor_surname"
        ],
        column_mapping=
        {
            "professor_id": "uid",
            "professor_name": "name",
            "professor_surname": "surname"
        }
    )
)

DatasetConfiguration.CURRICULA = DatasetConfiguration(
    dataset=DatasetType.CURRICULA,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.CURRICULA_INPUT),
    transformation_config=DatasetTransformationConfiguration(
        columns=
        [
            "curriculum_id",
            "course_type",
            "course_semester_season",
            "course_academic_year",
            "course_semester"
        ],
        column_mapping=
        {
            "curriculum_id": "uid",
            "course_type": "type",
            "course_semester_season": "semester_season",
            "course_academic_year": "academic_year",
            "course_semester": "semester"
        }
    )
)

DatasetConfiguration.REQUISITES = DatasetConfiguration(
    dataset=DatasetType.REQUISITES,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.REQUISITES_INPUT),
    transformation_config=DatasetTransformationConfiguration(
        columns=
        [
            "requisite_id",
            "course_prerequisite_type",
            "minimum_required_number_of_courses"
        ],
        column_mapping=
        {
            "requisite_id": "uid",
            "course_prerequisite_type": "type",
            "minimum_required_number_of_courses": "minimum_required_number_of_courses"
        }
    )
)
DatasetConfiguration.OFFERS = DatasetConfiguration(
    dataset=DatasetType.OFFERS,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.OFFERS_INPUT),
    transformation_config=DatasetTransformationConfiguration(
        columns=
        [
            "offers_id",
            "curriculum_id",
            "study_program_id"
        ],
        column_mapping=
        {
            "offers_id": "uid",
            "curriculum_id": "curriculum_id",
            "study_program_id": "study_program_id"
        }
    )
)
DatasetConfiguration.INCLUDES = DatasetConfiguration(
    dataset=DatasetType.INCLUDES,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.INCLUDES_INPUT),
    transformation_config=DatasetTransformationConfiguration(
        columns=
        [
            "includes_id",
            "curriculum_id",
            "course_id"
        ],
        column_mapping=
        {
            "includes_id": "uid",
            "curriculum_id": "curriculum_id",
            "course_id": "course_id"
        }
    )
)
DatasetConfiguration.PREREQUISITES = DatasetConfiguration(
    dataset=DatasetType.PREREQUISITES,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.PREREQUISITES_INPUT),
    transformation_config=DatasetTransformationConfiguration(
        columns=
        [
            "prerequisite_id",
            "prerequisite_course_id",
            "requisite_id"
        ],
        column_mapping=
        {
            "prerequisite_id": "uid",
            "prerequisite_course_id": "prerequisite_course_id",
            "requisite_id": "requisite_id"
        }
    )
)
DatasetConfiguration.POSTREQUISITES = DatasetConfiguration(
    dataset=DatasetType.POSTREQUISITES,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.POSTREQUISITES_INPUT),
    transformation_config=DatasetTransformationConfiguration(
        columns=
        [
            "postrequisite_id",
            "course_id",
            "requisite_id"
        ],
        column_mapping=
        {
            "postrequisite_id": "uid",
            "course_id": "course_id",
            "requisite_id": "requisite_id"
        }
    )
)
DatasetConfiguration.TEACHES = DatasetConfiguration(
    dataset=DatasetType.TEACHES,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.TEACHES_INPUT),
    transformation_config=DatasetTransformationConfiguration(
        columns=
        [
            "teaches_id",
            "course_id",
            "professor_id"
        ],
        column_mapping=
        {
            "teaches_id": "uid",
            "course_id": "course_id",
            "professor_id": "professor_id"
        }
    )
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

    def __init__(self, label: str, columns: list[str], index_column ="uid"):
        self.label = label
        self.columns = columns
        self.index_column = index_column

NodeIngestionConfiguration.STUDY_PROGRAMS = NodeIngestionConfiguration(
    label="StudyProgram",
    columns=list(DatasetConfiguration.STUDY_PROGRAMS.transformation_config.column_mapping.values())
)
NodeIngestionConfiguration.COURSES = NodeIngestionConfiguration(
    label="Course",
    columns=list(DatasetConfiguration.COURSES.transformation_config.column_mapping.values())
)
NodeIngestionConfiguration.PROFESSORS = NodeIngestionConfiguration(
    label="Professor",
    columns=list(DatasetConfiguration.PROFESSORS.transformation_config.column_mapping.values())
)
NodeIngestionConfiguration.CURRICULA = NodeIngestionConfiguration(
    label="Curriculum",
    columns=list(DatasetConfiguration.CURRICULA.transformation_config.column_mapping.values())
)
NodeIngestionConfiguration.REQUISITES = NodeIngestionConfiguration(
    label="Requisite",
    columns=list(DatasetConfiguration.REQUISITES.transformation_config.column_mapping.values())
)


class RelationshipIngestionConfiguration:
    OFFERS: "RelationshipIngestionConfiguration"
    INCLUDES: "RelationshipIngestionConfiguration"
    PREREQUISITES: "RelationshipIngestionConfiguration"
    POSTREQUISITES: "RelationshipIngestionConfiguration"
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
    columns=list(DatasetConfiguration.OFFERS.transformation_config.column_mapping.values())
)

RelationshipIngestionConfiguration.INCLUDES = RelationshipIngestionConfiguration(
    source_node_label="Curriculum",
    destination_node_label="Course",
    source_node_column=PartitioningConfiguration.INCLUDES.source_node_column,
    destination_node_column=PartitioningConfiguration.INCLUDES.destination_node_column,
    label="INCLUDES",
    columns=list(DatasetConfiguration.INCLUDES.transformation_config.column_mapping.values())
)

RelationshipIngestionConfiguration.PREREQUISITES = RelationshipIngestionConfiguration(
    source_node_label="Course",
    destination_node_label="Requisite",
    source_node_column=PartitioningConfiguration.PREREQUISITES.source_node_column,
    destination_node_column=PartitioningConfiguration.PREREQUISITES.destination_node_column,
    label="IS_PREREQUISITE_FOR",
    columns=list(DatasetConfiguration.PREREQUISITES.transformation_config.column_mapping.values())
)

RelationshipIngestionConfiguration.POSTREQUISITES = RelationshipIngestionConfiguration(
    source_node_label="Course",
    destination_node_label="Requisite",
    source_node_column=PartitioningConfiguration.POSTREQUISITES.source_node_column,
    destination_node_column=PartitioningConfiguration.POSTREQUISITES.destination_node_column,
    label="IS_POSTREQUISITE_FOR",
    columns=list(DatasetConfiguration.POSTREQUISITES.transformation_config.column_mapping.values())
)

RelationshipIngestionConfiguration.TEACHES = RelationshipIngestionConfiguration(
    source_node_label="Professor",
    destination_node_label="Course",
    source_node_column=PartitioningConfiguration.TEACHES.source_node_column,
    destination_node_column=PartitioningConfiguration.TEACHES.destination_node_column,
    label="TEACHES",
    columns=list(DatasetConfiguration.TEACHES.transformation_config.column_mapping.values())
)