from neomodel import StructuredRel

from src.config import Config
from src.models.nodes import StudyProgram, Course, Professor, BaseStructuredNode
from src.models.relationships import Curriculum, Prerequisite, Teaches
from src.patterns.strategy.processing.base_processing import RelationshipProcessingStrategy


class CurriculumDataProcessingStrategy(RelationshipProcessingStrategy):
    MODEL: StructuredRel = Curriculum
    RELATIONSHIP_PROPERTY_NAME = 'courses'
    SOURCE_NODE_MODEL: BaseStructuredNode = StudyProgram
    TARGET_NODE_MODEL: BaseStructuredNode = Course
    SOURCE_NODE_COLUMN: str = "study_program_id"
    TARGET_NODE_COLUMN: str = "course_id"
    FIELD_MAPPING: dict[str, str] = {
        "course_level": "level",
        "course_type": "type",
        "course_semester": "semester",
        "course_season": "season",
        "course_academic_year": "academic_year"
    }
    COLUMNS: list[str] = Config.CURRICULUM_COLUMNS
    GROUP_BY_COLUMN: str = "study_program_id"


class PrerequisiteDataProcessingStrategy(RelationshipProcessingStrategy):
    MODEL: StructuredRel = Prerequisite
    RELATIONSHIP_PROPERTY_NAME = 'prerequisite'
    SOURCE_NODE_MODEL: BaseStructuredNode = Course
    TARGET_NODE_MODEL: BaseStructuredNode = Course
    SOURCE_NODE_COLUMN: str = "course_id"
    TARGET_NODE_COLUMN: str = "course_prerequisites_course_id"
    FIELD_MAPPING: dict[str, str] = {
        "course_prerequisite_type": "type",
        "course_prerequisites_minimum_required_courses": "number_of_courses_required"
    }
    COLUMNS: list[str] = Config.PREREQUISITE_COLUMNS
    PREDICATE: callable = lambda df: df['course_prerequisite_type'] != 'NO_PREREQUISITE'
    GROUP_BY_COLUMN: str = "course_id"


class TeachesDataProcessingStrategy(RelationshipProcessingStrategy):
    MODEL: StructuredRel = Teaches
    RELATIONSHIP_PROPERTY_NAME = 'taught_by'
    SOURCE_NODE_MODEL: BaseStructuredNode = Course
    TARGET_NODE_MODEL: BaseStructuredNode = Professor
    SOURCE_NODE_COLUMN: str = "course_id"
    TARGET_NODE_COLUMN: str = "course_professors_id"
    FIELD_MAPPING: dict[str, str] = {}
    COLUMNS: list[str] = Config.TEACHES_COLUMNS
    PREDICATE: callable = lambda df: df['course_professors_id'] != 58
    GROUP_BY_COLUMN: str = "course_id"
