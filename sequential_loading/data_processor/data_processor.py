from sequential_loading.data_storage.data_storage import DataStorage
from sequential_loading.data_collector import DataCollector

from abc import ABC, abstractmethod

import pandas as pd
from typedframe import TypedDataFrame

from typing import List, Type, Dict, TypedDict, TypeVar, Generic, Callable, Mapping

import logging

import datetime

import uuid


class DataProcessor(ABC):
    def __init__(self, name: str, paramschema: Type[TypedDataFrame], schema: Type[TypedDataFrame], metaschema: Type[TypedDataFrame], storage: DataStorage, collectors: List[DataCollector]):
        #convert types into dataframes (schemas)
        self.name = name

        #defines behavior for cumulatively stored metadata
        self.update_map = Dict[str, Callable]

        self.metaschema = metaschema

        self.paramschema = paramschema
        self.schema = type('ProcessorSchema', (TypedDataFrame,), {"schema": {**paramschema.schema, **schema.schema}})
        self.metaschema = type('ProcessorMetaSchema', (TypedDataFrame,), {"schema": {**paramschema.schema, **metaschema.schema}})
        
        self.collectors = collectors

        self.storage: DataStorage = storage
        self.initialize()

        self.data: Type[TypedDataFrame] = None
        self.cached_metadata: Type[TypedDataFrame] = None

        self.cached_metadata = self.storage.retrieve_data(f"{self.name}_metadata")

        if self.cached_metadata.empty:
            self.cached_metadata = None

        if self.cached_metadata is not None:
            metaschema(self.cached_metadata)

        self.logger = logging.getLogger(__name__)

    "Get cached metadata"
    def format_query(self, **parameters: dict) -> str:
        #parameters are strings only
        #replaced "and" with "&" to match pandas documentation (I think). Change back if this breaks something.
        parameter_query = ' & '.join([f'{key} == "{value}"' for key, value in parameters.items()])
        return parameter_query

    @abstractmethod
    def initialize(self, **parameters: dict) -> None:
        pass

    "Updates metadata"
    @abstractmethod
    def update_metadata(self, parameters: Type[TypedDataFrame], metadata: Type[TypedDataFrame]) -> None:
        pass
    
    @abstractmethod
    def collect(self, collectors: List[DataCollector], **parameters) -> Type[TypedDataFrame]:
        pass

    @abstractmethod
    def delete(self, **parameters) -> Type[TypedDataFrame]:
        pass



"""
Interface for synthesizing data and metadata from data collectors.

Members
-------
parame_schema: pd.DataFrame
    Parameters that can be used to query data collectors.

meta_schema: pd.DataFrame
    The columns/datatypes stored as the metadata for a processor

schema: pd.DataFrame
    The columns/datatypes stored as the main data for a processor

metaschema: pd.DataFrame
    The 


data: pd.DataFrame
    Temporary storage for data. Reset to schema after storage.

metadata: pd.DataFrame
    Temporary storage for metadata. Reset to metaschema after storage.


Methods
-------

collect: (domain: SparsityMapping, collectors: list[DataCollector], **parameters: dict[str, str]) -> pd.DataFrame
    Collects data from DataCollectors and stores in data. Stores metadata about collected metadata.

collect_all: (domain: SparsityMapping) -> pd.DataFrame
    Collects data from all DataCollectors and stores in data. Stores metadata about collected metadata.

delete: (domain: SparsityMapping, collectors: list[DataCollector], **parameters: dict[str, str]) -> None
    Deletes data from DataCollectors and updates metadata.


"""
