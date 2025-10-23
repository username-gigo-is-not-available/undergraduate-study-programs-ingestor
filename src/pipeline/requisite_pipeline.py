from src.configurations import REQUISITES
from src.models.enums import StageType
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep


def requisites_pipeline():
    return (
        Pipeline(name='requisites-pipeline')
        .add_stage(
            PipelineStage(
                name='load-data',
                stage_type=StageType.LOAD
            )
            .add_step(
                PipelineStep(
                    name='load-requisites-data',
                    function=PipelineStep.read_data,
                    configuration=REQUISITES
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
                    column_mapping=REQUISITES.column_mapping
                )
            )
        )
        .add_stage(
            PipelineStage(
                name='cast-data',
                stage_type=StageType.CAST
            )
            .add_step(
                PipelineStep(
                    name='cast-uuid-column-to-string',
                    function=PipelineStep.cast,
                    column='uid',
                    data_type=str
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
                    configuration=REQUISITES
                )
            )
        )
    )
