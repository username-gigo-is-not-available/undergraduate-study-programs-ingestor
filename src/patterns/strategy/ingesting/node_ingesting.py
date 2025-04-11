from src.patterns.strategy.ingesting.base_ingesting import NodeIngestingStrategy


class StudyProgramDataIngestingStrategy(NodeIngestingStrategy):
    NODE_LABEL: str = "StudyProgram"
    INDEX_COLUMN: int = "uid"


class CourseDataIngestingStrategy(NodeIngestingStrategy):
    NODE_LABEL: str = "Course"
    INDEX_COLUMN: int = "uid"


class ProfessorDataIngestingStrategy(NodeIngestingStrategy):
    NODE_LABEL: str = "Professor"
    INDEX_COLUMN: int = "uid"
