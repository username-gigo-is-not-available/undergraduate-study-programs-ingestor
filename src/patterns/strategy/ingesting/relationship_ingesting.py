from src.patterns.strategy.ingesting.base_ingesting import RelationshipIngestingStrategy


class OffersDataIngestingStrategy(RelationshipIngestingStrategy):
    SOURCE_TARGET_RELATIONSHIP_PROPERTY_NAME = 'OFFERS'
    TARGET_SOURCE_RELATIONSHIP_PROPERTY_NAME = 'OFFERED_BY'
    SOURCE_NODE_LABEL: str = "StudyProgram"
    TARGET_NODE_LABEL: str = "Course"
    SOURCE_NODE_COLUMN: str = "study_program_id"
    TARGET_NODE_COLUMN: str = "course_id"


class RequiresDataIngestingStrategy(RelationshipIngestingStrategy):
    SOURCE_TARGET_RELATIONSHIP_PROPERTY_NAME = 'HAS_PREREQUISITE_FOR'
    TARGET_SOURCE_RELATIONSHIP_PROPERTY_NAME = 'IS_PREREQUISITE_FOR'
    SOURCE_NODE_LABEL: str = "Course"
    TARGET_NODE_LABEL: str = "Course"
    SOURCE_NODE_COLUMN: str = "course_id"
    TARGET_NODE_COLUMN: str = "course_prerequisite_id"


class TeachesDataIngestingStrategy(RelationshipIngestingStrategy):
    SOURCE_TARGET_RELATIONSHIP_PROPERTY_NAME = 'TEACHES'
    TARGET_SOURCE_RELATIONSHIP_PROPERTY_NAME = 'TAUGHT_BY'
    SOURCE_NODE_LABEL: str = "Professor"
    TARGET_NODE_LABEL: str = "Course"
    SOURCE_NODE_COLUMN: str = "professor_id"
    TARGET_NODE_COLUMN: str = "course_id"
