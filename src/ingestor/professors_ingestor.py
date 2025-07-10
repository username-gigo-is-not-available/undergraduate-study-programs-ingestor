from src.configurations import DatasetConfiguration, NodeIngestionConfiguration
from src.models.enums import StageType
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep


def professors_ingestor():
    return (Pipeline(name='professors-ingestor')
    .add_stage(
        PipelineStage(
            name='load-data',
            stage_type=StageType.LOAD
        )
        .add_step(
            PipelineStep(
                name='load-professors-data',
                function=PipelineStep.read_data,
                configuration=DatasetConfiguration.PROFESSORS
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
                name='rename-professors-columns',
                function=PipelineStep.rename,
                column_mapping=DatasetConfiguration.PROFESSORS.transformation_config.column_mapping
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
                name='ingest-professors-data',
                function=PipelineStep.ingest_nodes,
                configuration=NodeIngestionConfiguration.PROFESSORS
            )
        )
    )
    )
