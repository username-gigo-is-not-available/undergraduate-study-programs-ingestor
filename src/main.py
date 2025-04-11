import asyncio
import logging
import time

import pandas as pd
from neomodel.async_.core import AsyncDatabase

from src.enums import ComponentType, ComponentName
from src.patterns.mixin.database import DatabaseMixin
from src.patterns.mixin.ingesting import IngestingMixin
from src.patterns.mixin.processing import ProcessingMixin

logging.basicConfig(level=logging.INFO)


async def main():
    logging.info("Starting...")
    start = time.perf_counter()

    db: AsyncDatabase = await DatabaseMixin.connect()

    await DatabaseMixin.clear_database(db)

    component_names: list[ComponentName] = [
        ComponentName.STUDY_PROGRAM,
        ComponentName.COURSE,
        ComponentName.PROFESSOR,
        ComponentName.CURRICULUM,
        ComponentName.PREREQUISITE,
        ComponentName.TEACHES,
    ]

    node_components = list(filter(lambda x: ComponentName.get_component_type(x) == ComponentType.NODE, component_names))
    relationship_components = list(filter(lambda x: ComponentName.get_component_type(x) == ComponentType.RELATIONSHIP, component_names))
    processing_tasks: dict[ComponentName, asyncio.Task] = {
        component_name: asyncio.create_task(ProcessingMixin(component_name).run()) for component_name in component_names
    }

    processing_tasks: dict[ComponentName, pd.DataFrame] = dict(
        zip(processing_tasks.keys(), await asyncio.gather(*processing_tasks.values())))  # type: ignore

    ingesting_tasks: dict[ComponentName, asyncio.Task] = {
        component_name: asyncio.create_task(IngestingMixin(component_name).run(processing_tasks[component_name], db)) for component_name in
        node_components
    }

    await asyncio.gather(*ingesting_tasks.values())

    ingesting_tasks: dict[ComponentName, asyncio.Task] = {
        component_name: asyncio.create_task(IngestingMixin(component_name).run(processing_tasks[component_name], db)) for component_name in
        relationship_components
    }

    await asyncio.gather(*ingesting_tasks.values())

    await DatabaseMixin.disconnect(db)

    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
