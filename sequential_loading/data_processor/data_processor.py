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
    def __init__(self, name: str, paramschema: pd.DataFrame, schema: pd.DataFrame, metaschema: Type[TypedDataFrame], storage: DataStorage, collectors: List[DataCollector]):
        #convert types into dataframes (schemas)
        self.name = name

        #defines behavior for cumulatively stored metadata
        self.update_map = Dict[str, Callable]

        self.metaschema = metaschema

        self.paramschema = paramschema
        self.schema = type('ProcessorSchema', (paramschema, schema), {})
        self.metaschema = type('ProcessorMetaSchema', (paramschema, metaschema), {})
        
        self.collectors = collectors

        self.storage: DataStorage = storage
        self.storage.initialize(self.name, self.schema)
        self.storage.initialize(f"{self.name}_metadata", self.metaschema)

        metadata = self.storage.retrieve_data(f"{self.name}_metadata")
        if metadata:
            self.metadata = metaschema(metadata)

        self.logger = logging.getLogger(__name__)

    "Returns updated metadata"
    @abstractmethod
    def update_metadata(self, parameters: Type[TypedDataFrame], metadata: Type[TypedDataFrame]) -> Type[TypedDataFrame]:
        pass
    
    @abstractmethod
    def collect(self, parameters, collectors: List[DataCollector]) -> Type[TypedDataFrame]:
        pass

    @abstractmethod
    def delete(self, parameters, collectors: List[DataCollector]) -> Type[TypedDataFrame]:
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
