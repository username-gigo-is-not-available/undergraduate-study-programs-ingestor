from src.configurations import DatasetConfiguration, NodeIngestionConfiguration
from src.models.enums import StageType
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep


def requisites_ingestor():
    return (Pipeline(name='requisites-ingestor')
    .add_stage(
        PipelineStage(
            name='load-data',
            stage_type=StageType.LOAD
        )
        .add_step(
            PipelineStep(
                name='load-requisites-data',
                function=PipelineStep.read_data,
                configuration=DatasetConfiguration.REQUISITES
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
                name='rename-requisites-columns',
                function=PipelineStep.rename,
                column_mapping=DatasetConfiguration.REQUISITES.transformation_config.column_mapping
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
                name='ingest-requisites-data',
                function=PipelineStep.ingest_nodes,
                configuration=NodeIngestionConfiguration.REQUISITES
            )
        )
    )
    )
