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
        self.schema = type('ProcessorSchema', (TypedDataFrame,), {"schema": {**paramschema.schema, **schema.schema}, "unique_constraint": schema.unique_constraint if hasattr(schema, "unique_constraint") else None})
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
    
    #this function is actually not unique to IntervalProcessor, but could be used by all DataProcessors
    #The reason we pass in data as a dataframe is to preserve typing
    def update_metadata(self, parameters: pd.DataFrame, metadata: pd.DataFrame, deletion: bool = False) -> None:
        assert len(parameters) == 1, "Parameters must be a single row dataframe."
        assert len(metadata) == 1, "Metadata must be a single row dataframe."

        metadata = pd.concat([parameters, metadata], axis=1)
        self.metaschema(metadata)

        #cache metadata to avoid unnecessary queries to storage
        if self.cached_metadata is None:
            self.cached_metadata = metadata
            return metadata
        
        parameters = parameters.iloc[0].to_dict()

        cached_metadata, metadata_index = self.retrieve_metadata(**parameters)

        if cached_metadata is None:
            self.cached_metadata = pd.concat([self.cached_metadata, metadata], ignore_index=True)
            return metadata

        for key, value in self.update_map.items():
            cached_metadata[key] = value(cached_metadata[key], metadata.loc[0, key], deletion)
        
        #Get index of existing metadata
        self.cached_metadata.loc[metadata_index] = cached_metadata

        return cached_metadata

    #This is also not specific to IntervalProcessor, but could be used by all DataProcessors
    def update_data(self, parameters: pd.DataFrame, data: Type[TypedDataFrame]) -> pd.DataFrame:  
        data = pd.concat([parameters, data], axis=1)
        self.schema(data)
        self.data = data

        return self.data
    
    #also not specific, will move all these eventually
    def retrieve_metadata(self, **parameters: dict) -> dict:
        if self.cached_metadata is None:
            return (None, None)

        parameter_query = ' & '.join([f'{key} == "{value}"' for key, value in parameters.items()])
        results = self.cached_metadata.query(parameter_query)
        
        if results.empty:
            return (None, None)
        
        return results.iloc[0], results.index[0]

    @abstractmethod
    def initialize(self, **parameters: dict) -> None:
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
