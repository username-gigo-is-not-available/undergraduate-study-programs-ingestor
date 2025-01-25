from src.models.enums import ComponentName
from src.patterns.strategy.processing.node_processing import StudyProgramDataProcessingStrategy, CourseDataProcessingStrategy, \
    ProfessorDataProcessingStrategy
from src.patterns.strategy.processing.relationship_processing import CurriculumDataProcessingStrategy, \
    PrerequisiteDataProcessingStrategy, TeachesDataProcessingStrategy


class ProcessingMixin:
    def __init__(self, component_name: ComponentName):
        if component_name == ComponentName.STUDY_PROGRAM:
            self.data_processing_strategy = StudyProgramDataProcessingStrategy()
        elif component_name == ComponentName.COURSE:
            self.data_processing_strategy = CourseDataProcessingStrategy()
        elif component_name == ComponentName.PROFESSOR:
            self.data_processing_strategy = ProfessorDataProcessingStrategy()
        elif component_name == ComponentName.CURRICULUM:
            self.data_processing_strategy = CurriculumDataProcessingStrategy()
        elif component_name == ComponentName.PREREQUISITE:
            self.data_processing_strategy = PrerequisiteDataProcessingStrategy()
        elif component_name == ComponentName.TEACHES:
            self.data_processing_strategy = TeachesDataProcessingStrategy()
        else:
            raise ValueError(f"Unsupported component name: {component_name}")

    def run(self):
        self.data_processing_strategy.run()
