from src.configurations import RelationshipIngestionConfiguration, DatasetConfiguration, PartitioningConfiguration
from src.models.enums import StageType
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep


def satisfies_ingestor():
    return (
        Pipeline(name='satisfies-ingestor')
    .add_stage(
        PipelineStage(
            name='load-data',
            stage_type=StageType.LOAD
        )
        .add_step(
            PipelineStep(
                name='load-prerequisites-data',
                function=PipelineStep.read_data,
                configuration=DatasetConfiguration.SATISFIES
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
                name='rename-satisfies-columns',
                function=PipelineStep.rename,
                column_mapping=DatasetConfiguration.SATISFIES.transformation_config.column_mapping
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
                configuration=PartitioningConfiguration.PREREQUISITES
            )
        )
        .add_step(
            PipelineStep(
                name='partition-satisfies-data',
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
                name='ingest-satisfies-data',
                function=PipelineStep.ingest_relationships,
                configuration=RelationshipIngestionConfiguration.SATISFIES
            )
        )
    )
    )
