import asyncio
import logging
import time

from src.configurations import NodeConfiguration, STUDY_PROGRAMS, CURRICULA, COURSES, REQUISITES, PROFESSORS
from src.pipeline.courses_pipeline import courses_pipeline
from src.pipeline.curricula_pipeline import curricula_pipeline
from src.pipeline.includes_pipeline import includes_pipeline
from src.pipeline.offers_pipeline import offers_pipeline
from src.pipeline.requires_pipeline import requires_pipeline
from src.pipeline.satisfies_pipeline import satisfies_pipeline
from src.pipeline.professors_pipeline import professors_pipeline
from src.pipeline.requisite_pipeline import requisites_pipeline
from src.pipeline.study_programs_pipeline import study_programs_pipeline
from src.pipeline.teaches_pipeline import teaches_pipeline
from src.storage import Neo4jClient
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

        node_pipelines: list[Pipeline] = [
            study_programs_pipeline(),
            courses_pipeline(),
            professors_pipeline(),
            curricula_pipeline(),
            requisites_pipeline()
        ]

        relationship_pipelines: list[Pipeline] = [
            offers_pipeline(),
            includes_pipeline(),
            satisfies_pipeline(),
            requires_pipeline(),
            teaches_pipeline()
        ]

        node_ingestion_tasks: list[asyncio.Task[Pipeline]] = [asyncio.create_task(pipeline.build().run()) for pipeline
                                                              in node_pipelines]
        await asyncio.gather(*node_ingestion_tasks)

        create_indices_tasks: list[asyncio.Task[None]] = [
            asyncio.create_task(Neo4jClient.create_index(configuration.label, configuration.index_column)) for
            configuration in [
                STUDY_PROGRAMS,
                CURRICULA,
                COURSES,
                REQUISITES,
                PROFESSORS,
            ]
        ]

        await asyncio.gather(*create_indices_tasks)

        relationship_ingestion_tasks: list[asyncio.Task[Pipeline]] = [asyncio.create_task(pipeline.build().run()) for
                                                                      pipeline in relationship_pipelines]
        await asyncio.gather(*relationship_ingestion_tasks)


    finally:

        await Neo4jClient.disconnect()

    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
