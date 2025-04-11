import pandas as pd
from neomodel.async_.core import AsyncDatabase

from src.models.enums import ComponentName
from src.patterns.strategy.ingesting.node_ingesting import StudyProgramDataIngestingStrategy, CourseDataIngestingStrategy, \
    ProfessorDataIngestingStrategy
from src.patterns.strategy.ingesting.relationship_ingesting import CurriculumDataIngestingStrategy, PrerequisiteDataIngestingStrategy, \
    TeachesDataIngestingStrategy


class IngestingMixin:
    def __init__(self, component_name: ComponentName):
        if component_name == ComponentName.STUDY_PROGRAM:
            self.ingesting_strategy = StudyProgramDataIngestingStrategy()
        elif component_name == ComponentName.COURSE:
            self.ingesting_strategy = CourseDataIngestingStrategy()
        elif component_name == ComponentName.PROFESSOR:
            self.ingesting_strategy = ProfessorDataIngestingStrategy()
        elif component_name == ComponentName.CURRICULUM:
            self.ingesting_strategy = CurriculumDataIngestingStrategy()
        elif component_name == ComponentName.PREREQUISITE:
            self.ingesting_strategy = PrerequisiteDataIngestingStrategy()
        elif component_name == ComponentName.TEACHES:
            self.ingesting_strategy = TeachesDataIngestingStrategy()
        else:
            raise ValueError(f"Unsupported component name: {component_name}")

    async def run(self, df: pd.DataFrame, db: AsyncDatabase) -> None:
        await self.ingesting_strategy.run(df, db)
