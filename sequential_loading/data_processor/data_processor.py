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
        print(paramschema.schema)
        self.name = name

        #defines behavior for cumulatively stored metadata
        self.update_map = Dict[str, Callable]

        self.metaschema = metaschema

        self.schema = type('ProcessorSchema', (TypedDataFrame,), {"schema": {**paramschema.schema, **schema.schema}})
        self.metaschema = type('ProcessorMetaSchema', (TypedDataFrame,), {"schema": {**paramschema.schema, **metaschema.schema}})
        
        self.collectors = collectors

        # self.data: pd.DataFrame = self.schema(self.schema.schema)
        # self.metadata: pd.DataFrame = self.metaschema(self.metaschema.schema)

        self.storage: DataStorage = storage
        self.storage.initialize(self.name, self.schema, self.metaschema)

        metadata = self.storage.retrieve_metadata(self)
        self.metadata = pd.concat(self.metadata, metadata, verify_integrity=True)

        self.update_map = self.configure_update_map()

        self.logger = logging.getLogger(__name__)

    """
    This function will be called every time data is collected with parameters.

    it will take in a set of parameters and optional metadata. It will use the self.update function to update the metadata and parameters accordingly.
    """
    def update_metadata(self, parameters, info = None) -> Type[TypedDataFrame]:
        #retrieve existing metadata from parameters
        existing_metadata: Type[TypedDataFrame] = self.metadata.query(' & '.join([f'{key} == {value}' for key, value in parameters.items()]))

        if not existing_metadata:
            updated_metadata = self.default_metadata_initialize(parameters, info)
            return updated_metadata

        for key, value in {**parameters, **info}.items():
            updater = self.update_map.get(key, self.default_metadata_update)
            existing_metadata[key] = updater(value, existing_metadata[key])

        self.storage.store_metadata(self.name, existing_metadata)
        return existing_metadata
    
    def default_metadata_update(self, parameters, info) -> Type[TypedDataFrame]:
        return pd.DataFrame({**parameters, **info})
    
    def default_metadata_initialize(self, parameters, info = None) -> Type[TypedDataFrame]:
        self.metadata[self.metadata.query(' & '.join([f'{key} == {value}' for key, value in parameters.items()]))] = info
        return self.metadata
    
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
