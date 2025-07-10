from src.configurations import DatasetConfiguration, NodeIngestionConfiguration
from src.models.enums import StageType
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep


def study_programs_ingestor():
    return (Pipeline(name='study-programs-ingestor')
    .add_stage(
        PipelineStage(
            name='load-data',
            stage_type=StageType.LOAD
        )
        .add_step(
            PipelineStep(
                name='load-study-programs-data',
                function=PipelineStep.read_data,
                configuration=DatasetConfiguration.STUDY_PROGRAMS
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
                name='rename-study-programs-columns',
                function=PipelineStep.rename,
                column_mapping=DatasetConfiguration.STUDY_PROGRAMS.transformation_config.column_mapping
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
                name='ingest-study-programs-data',
                function=PipelineStep.ingest_nodes,
                configuration=NodeIngestionConfiguration.STUDY_PROGRAMS
            )
        )
    )
    )
