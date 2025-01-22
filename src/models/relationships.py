from neomodel import StringProperty, IntegerProperty, StructuredRel


class Curriculum(StructuredRel):
    level = IntegerProperty()
    type = StringProperty()
    semester = IntegerProperty()
    season = StringProperty()
    academic_year = IntegerProperty()


class Prerequisite(StructuredRel):
    type = StringProperty()
    number_of_courses = IntegerProperty(default=0)


class Teaches(StructuredRel):
    pass


