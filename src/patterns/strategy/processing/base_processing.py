import logging

import pandas as pd
from neo4j.exceptions import TransientError
from neomodel import db, StructuredRel, RelationshipManager, ZeroOrMore
from pandas.core.groupby import DataFrameGroupBy
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

from src.config import Config
from src.db import connect_to_neo4j
from src.models.enums import ComponentType
from src.models.nodes import BaseStructuredNode
from src.patterns.mixin.file_storage import FileStorageMixin


class BaseProcessingStrategy(FileStorageMixin):
    DATA: pd.DataFrame = FileStorageMixin().read_data(Config.INPUT_FILE_NAME)
    COLUMNS: list[str] = []
    FIELD_MAPPING: dict[str, str] = {}
    MODEL: BaseStructuredNode | StructuredRel = None
    PREDICATE: callable = None
    GROUP_BY_COLUMN: str = None

    @classmethod
    def load(cls, unique: bool = True) -> pd.DataFrame:
        df: pd.DataFrame = cls.DATA[cls.COLUMNS]
        return df.drop_duplicates() if unique else df

    @classmethod
    def group_by(cls, df: pd.DataFrame) -> DataFrameGroupBy:
        return df.groupby(cls.GROUP_BY_COLUMN) if cls.GROUP_BY_COLUMN else df

    @classmethod
    def filter(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df[cls.PREDICATE(df)] if cls.PREDICATE else df

    @classmethod
    def get(cls, **kwargs):
        pass

    @classmethod
    def write(cls, row: dict[str, str | int], *args, **kwargs) -> None:
        pass

    @classmethod
    @connect_to_neo4j
    @retry(stop=stop_after_attempt(Config.DATABASE_RETRY_COUNT),
           wait=wait_random_exponential(multiplier=Config.DATABASE_RETRY_MULTIPLIER_IN_SECONDS,
                                        exp_base=Config.DATABASE_RETRY_EXPONENT_BASE),
           retry=retry_if_exception_type(TransientError))
    @db.transaction
    def process(cls, df: pd.DataFrame | DataFrameGroupBy) -> None:
        pass

    @classmethod
    def run(cls) -> None:
        cls.process(cls.group_by(cls.filter(cls.load())))


class NodeProcessingStrategy(BaseProcessingStrategy):
    COMPONENT_TYPE: ComponentType = ComponentType.NODE

    @classmethod
    def get(cls, **kwargs) -> BaseStructuredNode:
        return cls.MODEL.nodes.get(**kwargs)

    @classmethod
    def write(cls, row: dict[str, str | int], *args, **kwargs) -> BaseStructuredNode:
        node_data = {field: row[source] for source, field in cls.FIELD_MAPPING.items()}
        node: BaseStructuredNode = cls.MODEL.create(node_data)  # type: ignore
        logging.info(f"Created node: {node}")
        return node

    @classmethod
    @connect_to_neo4j
    @retry(stop=stop_after_attempt(max_attempt_number=Config.DATABASE_RETRY_COUNT),
           wait=wait_random_exponential(multiplier=Config.DATABASE_RETRY_MULTIPLIER_IN_SECONDS,
                                        exp_base=Config.DATABASE_RETRY_EXPONENT_BASE),
           retry=retry_if_exception_type(TransientError))
    @db.transaction
    def process(cls, df: pd.DataFrame) -> None:
        for row in df.to_dict(orient="records"):
            cls.write(row)


class RelationshipProcessingStrategy(BaseProcessingStrategy):
    COMPONENT_TYPE: ComponentType = ComponentType.RELATIONSHIP
    RELATIONSHIP_PROPERTY_NAME: str = None
    SOURCE_NODE_MODEL: BaseStructuredNode = None
    TARGET_NODE_MODEL: BaseStructuredNode = None
    SOURCE_NODE_COLUMN: str = None
    TARGET_NODE_COLUMN: str = None

    @classmethod
    def get(cls, **kwargs) -> StructuredRel | None:
        source_node: BaseStructuredNode = kwargs.get("source_node")
        target_node: BaseStructuredNode = kwargs.get("target_node")
        relationship_manager: RelationshipManager = source_node.get_relationship(cls.RELATIONSHIP_PROPERTY_NAME)
        if relationship_manager.is_connected(target_node):
            relationship_attributes = {key: kwargs.get(value) for key, value in cls.FIELD_MAPPING.items() if
                                       key not in ["source_node", "target_node"]}
            logging.info(f"Retrieved relationship between {source_node} and {target_node} with properties: {relationship_attributes}")
            return relationship_manager.match(kwargs=relationship_attributes).first()
        else:
            logging.info(f"Relationship between {source_node} and {target_node} does not exist")
            return None

    @classmethod
    def write(cls, row: dict[str, str | int], *args, **kwargs) -> None:
        source_node: BaseStructuredNode = kwargs.get("source_node")
        target_node: BaseStructuredNode = kwargs.get("target_node")
        relationship_manager: ZeroOrMore = source_node.get_relationship(cls.RELATIONSHIP_PROPERTY_NAME)
        if not relationship_manager.is_connected(target_node):
            relationship = relationship_manager.connect(target_node, cls.FIELD_MAPPING)
            logging.info(f"Created relationship between {source_node} and {target_node} with properties: {relationship}")
            return relationship
        else:
            logging.info(f"Relationship between {source_node} and {target_node} already exists")

    @classmethod
    @connect_to_neo4j
    @retry(stop=stop_after_attempt(Config.DATABASE_RETRY_COUNT),
           wait=wait_random_exponential(multiplier=Config.DATABASE_RETRY_MULTIPLIER_IN_SECONDS,
                                        exp_base=Config.DATABASE_RETRY_EXPONENT_BASE),
           retry=retry_if_exception_type(TransientError))
    @db.transaction
    def process(cls, df: DataFrameGroupBy) -> None:
        for source_node_id, group in df:
            source_node: BaseStructuredNode = cls.SOURCE_NODE_MODEL.nodes.get(uid=source_node_id)
            for row in group.to_dict(orient="records"):
                target_node_id: str = row.get(cls.TARGET_NODE_COLUMN)
                target_node: BaseStructuredNode = cls.TARGET_NODE_MODEL.nodes.get(uid=target_node_id)
                cls.write(row, source_node=source_node, target_node=target_node)
