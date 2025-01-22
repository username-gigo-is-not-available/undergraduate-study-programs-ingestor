from enum import StrEnum, auto


class UpperStrEnum(StrEnum):
    def _generate_next_value_(name, start, count, last_values):
        return name.upper()


class CoursePrerequisiteType(UpperStrEnum):
    REQUIRED_COURSE: str = auto()
    OPTIONAL_COURSES: str = auto()
    NO_PREREQUISITE: str = auto()
    MINIMUM_NUMBER_OF_COURSES_PASSED: str = auto()


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