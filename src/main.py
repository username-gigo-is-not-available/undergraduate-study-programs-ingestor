import asyncio
import logging
import time
from asyncio import Future
from concurrent.futures import ProcessPoolExecutor

from src.models.enums import ComponentType, ComponentName
from src.patterns.mixin.processing import ProcessingMixin
from src.patterns.strategy.processing.base_processing import NodeProcessingStrategy, \
    RelationshipProcessingStrategy, BaseProcessingStrategy

logging.basicConfig(level=logging.INFO)


async def main():
    logging.info("Starting...")
    start = time.perf_counter()
    components: list[ComponentName] = [
        ComponentName.STUDY_PROGRAM,
        ComponentName.COURSE,
        ComponentName.PROFESSOR,
        ComponentName.CURRICULUM,
        ComponentName.PREREQUISITE,
        ComponentName.TEACHES,
    ]
    processing_strategies: list[BaseProcessingStrategy] = [ProcessingMixin(component).data_processing_strategy for component in components]

    node_processing_strategies: list[NodeProcessingStrategy] = list(
        filter(lambda c: c.COMPONENT_TYPE == ComponentType.NODE, processing_strategies))
    relationship_processing_strategies: list[RelationshipProcessingStrategy] = list(
        filter(lambda c: c.COMPONENT_TYPE == ComponentType.RELATIONSHIP, processing_strategies))

    with ProcessPoolExecutor() as executor:
        node_futures: list[Future] = [executor.submit(strategy.run) for strategy in node_processing_strategies]
        for future in node_futures:
            future.result()
        relationship_future: list[Future] = [executor.submit(strategy.run) for strategy in relationship_processing_strategies]
        for future in relationship_future:
            future.result()

    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
