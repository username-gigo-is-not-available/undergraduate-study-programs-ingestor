from src.configurations import CURRICULA
from src.models.enums import StageType
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep


def curricula_pipeline():
    return (
        Pipeline(name='curricula-pipeline')
        .add_stage(
            PipelineStage(
                name='load-data',
                stage_type=StageType.LOAD
            )
            .add_step(
                PipelineStep(
                    name='load-curricula-data',
                    function=PipelineStep.read_data,
                    configuration=CURRICULA
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
                    name='rename-curricula-columns',
                    function=PipelineStep.rename,
                    column_mapping=CURRICULA.column_mapping
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
                    name='ingest-curricula-data',
                    function=PipelineStep.ingest_nodes,
                    configuration=CURRICULA
                )
            )
        )
    )
