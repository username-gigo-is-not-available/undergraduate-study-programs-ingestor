from src.configurations import DatasetConfiguration, NodeIngestionConfiguration
from src.models.enums import StageType
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep


def courses_ingestor():
    return (Pipeline(name='courses-ingestor')
    .add_stage(
        PipelineStage(
            name='load-data',
            stage_type=StageType.LOAD
        )
        .add_step(
            PipelineStep(
                name='load-courses-data',
                function=PipelineStep.read_data,
                configuration=DatasetConfiguration.COURSES
            )
        )
    )
    .add_stage(
        PipelineStage(
            name='rename-data',
            stage_type=StageType.RENAME
        )
        .add_step(
            PipelineStep(
                name='rename-courses-columns',
                function=PipelineStep.rename,
                column_mapping=DatasetConfiguration.COURSES.transformation_config.column_mapping
            )
        )
    )
    .add_stage(
        PipelineStage(
            name='ingest-data',
            stage_type=StageType.INGEST
        )
        .add_step(
            PipelineStep(
                name='ingest-courses-data',
                function=PipelineStep.ingest_nodes,
                configuration=NodeIngestionConfiguration.COURSES
            )
        )
    )
    )
