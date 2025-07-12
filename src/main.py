import asyncio
import logging
import time

from src.configurations import NodeIngestionConfiguration, RelationshipIngestionConfiguration
from src.ingestor.courses_ingestor import courses_ingestor
from src.ingestor.curricula_ingestor import curricula_ingestor
from src.ingestor.includes_ingestor import includes_ingestor
from src.ingestor.offers_ingestor import offers_ingestor
from src.ingestor.requires_ingestor import requires_ingestor
from src.ingestor.satisfies_ingestor import satisfies_ingestor
from src.ingestor.professors_ingestor import professors_ingestor
from src.ingestor.requisite_ingestor import requisites_ingestor
from src.ingestor.study_programs_ingestor import study_programs_ingestor
from src.ingestor.teaches_ingestor import teaches_ingestor
from src.clients import Neo4jClient
from src.patterns.builder.pipeline import Pipeline

logging.basicConfig(level=logging.INFO)


async def main():
    logging.info("Starting...")
    start = time.perf_counter()
    await Neo4jClient.verify_connection()
    try:

        await Neo4jClient.clear_database()
        await Neo4jClient.drop_constraints()
        await Neo4jClient.drop_indices()

        ingestion_configurations: list[NodeIngestionConfiguration | RelationshipIngestionConfiguration] = [
            NodeIngestionConfiguration.STUDY_PROGRAMS,
            NodeIngestionConfiguration.COURSES,
            NodeIngestionConfiguration.PROFESSORS,
            NodeIngestionConfiguration.CURRICULA,
            NodeIngestionConfiguration.REQUISITES,
        ]

        node_pipelines: list[Pipeline] = [
            study_programs_ingestor(),
            courses_ingestor(),
            professors_ingestor(),
            curricula_ingestor(),
            requisites_ingestor()
        ]

        relationship_pipelines: list[Pipeline] = [
            offers_ingestor(),
            includes_ingestor(),
            satisfies_ingestor(),
            requires_ingestor(),
            teaches_ingestor()
        ]

        node_ingestion_tasks: list[asyncio.Task[Pipeline]] = [asyncio.create_task(pipeline.build().run()) for pipeline
                                                              in node_pipelines]
        await asyncio.gather(*node_ingestion_tasks)

        create_indices_tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(Neo4jClient.create_index(configuration.label, configuration.index_column)) for
            configuration in ingestion_configurations]

        await asyncio.gather(*create_indices_tasks)

        relationship_ingestion_tasks: list[asyncio.Task[Pipeline]] = [asyncio.create_task(pipeline.build().run()) for
                                                                      pipeline in relationship_pipelines]
        await asyncio.gather(*relationship_ingestion_tasks)


    finally:

        await Neo4jClient.disconnect()

    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
