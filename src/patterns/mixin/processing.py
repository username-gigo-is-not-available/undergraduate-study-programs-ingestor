import pandas as pd

from src.models.enums import ComponentName
from src.patterns.strategy.processing.node_processing import StudyProgramProcessingStrategy, CourseProcessingStrategy, \
    ProfessorProcessingStrategy
from src.patterns.strategy.processing.relationship_processing import CurriculumProcessingStrategy, PrerequisiteProcessingStrategy, \
    TeachesProcessingStrategy


class ProcessingMixin:

    def __init__(self, component_name: ComponentName):
        if component_name == ComponentName.STUDY_PROGRAM:
            self.processing_strategy = StudyProgramProcessingStrategy()
        elif component_name == ComponentName.COURSE:
            self.processing_strategy = CourseProcessingStrategy()
        elif component_name == ComponentName.PROFESSOR:
            self.processing_strategy = ProfessorProcessingStrategy()
        elif component_name == ComponentName.CURRICULUM:
            self.processing_strategy = CurriculumProcessingStrategy()
        elif component_name == ComponentName.PREREQUISITE:
            self.processing_strategy = PrerequisiteProcessingStrategy()
        elif component_name == ComponentName.TEACHES:
            self.processing_strategy = TeachesProcessingStrategy()
        else:
            raise ValueError(f"Unsupported component name: {component_name}")

    async def run(self) -> pd.DataFrame | list[pd.DataFrame]:
        return await self.processing_strategy.run()
