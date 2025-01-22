
from neomodel import IntegerProperty, StringProperty, StructuredNode, RelationshipTo

from src.models.relationships import Curriculum, Prerequisite, Teaches


class BaseStructuredNode(StructuredNode):
    uid = IntegerProperty(unique_index=True, required=True)

    def get_relationship(self, name: str):
        return getattr(self, name, None)


class StudyProgram(BaseStructuredNode):
    name = StringProperty(required=True)
    duration = IntegerProperty(required=True)
    url = StringProperty(unique_index=True, required=True)
    courses = RelationshipTo("Course", "CURRICULUM", model=Curriculum)


class Course(BaseStructuredNode):
    name_mk = StringProperty(unique_index=True, required=True)
    name_en = StringProperty(unique_index=True, required=True)
    url = StringProperty(unique_index=True, required=True)
    prerequisite = RelationshipTo("Course", "HAS_PREREQUISITE", model=Prerequisite)
    taught_by = RelationshipTo("Professor", "TAUGHT_BY", model=Teaches)


class Professor(BaseStructuredNode):
    name = StringProperty(unique_index=True, required=True)

