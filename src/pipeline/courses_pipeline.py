from src.configurations import NodeConfiguration, COURSES
from src.models.enums import StageType
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep


def courses_pipeline():
    return (
        Pipeline(name='courses-pipeline')
        .add_stage(
            PipelineStage(
                name='load-data',
                stage_type=StageType.LOAD
            )
            .add_step(
                PipelineStep(
                    name='load-courses-data',
                    function=PipelineStep.read_data,
                    configuration=COURSES
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
                    column_mapping=COURSES.column_mapping
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
                    name='ingest-courses-data',
                    function=PipelineStep.ingest_nodes,
                    configuration=COURSES
                )
            )
        )
    )
