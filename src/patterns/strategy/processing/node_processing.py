from src.config import Config
from src.models.nodes import Professor, Course, StudyProgram
from src.patterns.strategy.processing.base_processing import NodeProcessingStrategy


class StudyProgramDataProcessingStrategy(NodeProcessingStrategy):
    MODEL = StudyProgram
    FIELD_MAPPING = {
        "study_program_id": "uid",
        "study_program_code": "code",
        "study_program_name": "name",
        "study_program_duration": "duration",
        "study_program_url": "url",
    }
    COLUMNS = Config.STUDY_PROGRAM_COLUMNS


class CourseDataProcessingStrategy(NodeProcessingStrategy):
    MODEL = Course
    FIELD_MAPPING = {
        "course_id": "uid",
        "course_code": "code",
        "course_name_mk": "name_mk",
        "course_name_en": "name_en",
        "course_url": "url",
    }
    COLUMNS = Config.COURSE_COLUMNS


class ProfessorDataProcessingStrategy(NodeProcessingStrategy):
    MODEL = Professor
    FIELD_MAPPING = {
        "course_professors_id": "uid",
        "course_professors": "name",
    }
    COLUMNS = Config.PROFESSOR_COLUMNS
    PREDICATE: callable = lambda df: df['course_professors'] != 'нема'
