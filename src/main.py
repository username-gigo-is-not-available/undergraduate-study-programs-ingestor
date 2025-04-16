import asyncio
import logging
import time

import pandas as pd
from neomodel.async_.core import AsyncDatabase

from src.models.enums import ComponentType, ComponentName
from src.models.models import Component
from src.clients import Neo4jClient
from src.patterns.mixin.ingesting import IngestingMixin
from src.patterns.mixin.processing import ProcessingMixin

logging.basicConfig(level=logging.INFO)


async def process_and_ingest(components: list[Component]) -> None:
    processing_tasks: dict[Component, asyncio.Task] = {
        component: asyncio.create_task(ProcessingMixin(component.component_name).run()) for component in components
    }
    data: dict[Component, pd.DataFrame] = dict(zip(processing_tasks.keys(), await asyncio.gather(*processing_tasks.values())))
    ingesting_tasks: dict[Component, asyncio.Task] = {
        component: asyncio.create_task(IngestingMixin(component.component_name).run(data[component])) for component in components
    }
    await asyncio.gather(*ingesting_tasks.values())


async def main():
    logging.info("Starting...")
    start = time.perf_counter()
    await Neo4jClient.verify_connection()
    try:

        await Neo4jClient.clear_database()

        components: list[Component] = [
            Component(ComponentName.STUDY_PROGRAM, ComponentType.NODE),
            Component(ComponentName.COURSE, ComponentType.NODE),
            Component(ComponentName.PROFESSOR, ComponentType.NODE),
            Component(ComponentName.CURRICULUM, ComponentType.RELATIONSHIP),
            Component(ComponentName.PREREQUISITE, ComponentType.RELATIONSHIP),
            Component(ComponentName.TEACHES, ComponentType.RELATIONSHIP)
        ]

        node_components: list[Component] = list(filter(lambda x: x.component_type == ComponentType.NODE, components))
        relationship_components: list[Component] = list(filter(lambda x: x.component_type == ComponentType.RELATIONSHIP, components))

        await process_and_ingest(node_components)
        await process_and_ingest(relationship_components)

    finally:

        await Neo4jClient.disconnect()

    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
