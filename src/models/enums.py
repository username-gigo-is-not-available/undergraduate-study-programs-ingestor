from enum import StrEnum, auto


class UpperStrEnum(StrEnum):
    def _generate_next_value_(name, start, count, last_values):
        return name.upper()


class CoursePrerequisiteType(UpperStrEnum):
    ONE: str = auto()
    ANY: str = auto()
    NONE: str = auto()
    TOTAL: str = auto()

class StageType(UpperStrEnum):
    LOAD = auto()
    RENAME = auto()
    PARTITION = auto()
    INGEST = auto()
    STORE = auto()

class ComponentType(StrEnum):
    NODE: str = auto()
    RELATIONSHIP: str = auto()


class ComponentName(StrEnum):
    STUDY_PROGRAM: str = auto()
    COURSE: str = auto()
    PROFESSOR: str = auto()
    CURRICULUM: str = auto()
    PREREQUISITE: str = auto()
    TEACHES: str = auto()

    @staticmethod
    def get_component_type(component_name: 'ComponentName') -> ComponentType:
        return {
            ComponentName.STUDY_PROGRAM: ComponentType.NODE,
            ComponentName.COURSE: ComponentType.NODE,
            ComponentName.PROFESSOR: ComponentType.NODE,
            ComponentName.CURRICULUM: ComponentType.RELATIONSHIP,
            ComponentName.PREREQUISITE: ComponentType.RELATIONSHIP,
            ComponentName.TEACHES: ComponentType.RELATIONSHIP,
        }[component_name]

class DatasetType(UpperStrEnum):
    STUDY_PROGRAMS: str = auto()
    COURSES: str = auto()
    PROFESSORS: str = auto()
    CURRICULA: str = auto()
    REQUISITES: str = auto()
    OFFERS: str = auto()
    INCLUDES: str = auto()
    PREREQUISITES: str = auto()
    POSTREQUISITES: str = auto()
    TEACHES: str = auto()