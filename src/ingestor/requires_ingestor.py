from src.configurations import RelationshipIngestionConfiguration, DatasetConfiguration, PartitioningConfiguration
from src.models.enums import StageType
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep


def requires_ingestor():
    return (
        Pipeline(name='requires-ingestor')
    .add_stage(
        PipelineStage(
            name='load-data',
            stage_type=StageType.LOAD
        )
        .add_step(
            PipelineStep(
                name='load-requires-data',
                function=PipelineStep.read_data,
                configuration=DatasetConfiguration.REQUIRES
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
                name='rename-requires-columns',
                function=PipelineStep.rename,
                column_mapping=DatasetConfiguration.REQUIRES.transformation_config.column_mapping
            )
        )
    )
    .add_stage(
        PipelineStage(
            name='partition-data',
            stage_type=StageType.PARTITION
        )
        .add_step(
            PipelineStep(
                name='generate-partition-id',
                function=PipelineStep.generate_partition_uid,
                configuration=PartitioningConfiguration.POSTREQUISITES
            )
        )
        .add_step(
            PipelineStep(
                name='partition-requires-data',
                function=PipelineStep.partition,
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
                name='ingest-requires-data',
                function=PipelineStep.ingest_relationships,
                configuration=RelationshipIngestionConfiguration.REQUIRES
            )
        )
    )
    )
