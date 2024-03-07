from sequential_loading.data_storage.data_storage import DataStorage
from sequential_loading.data_collector import DataCollector

from abc import ABC, abstractmethod

import pandas as pd

from typing import List, Type, TypedDict, TypeVar, Generic, Callable

import logging

import datetime

import uuid

SCHEMA = TypeVar("SCHEMA")
PARAMSCHEMA = TypeVar("PARAMSCHEMA")
METASCHEMA = TypeVar("METASCHEMA")


class DataProcessor(ABC, Generic[PARAMSCHEMA, METASCHEMA, SCHEMA]):
    def __init__(self, storage: DataStorage, collectors: List[DataCollector]):
        #convert types into dataframes (schemas)
        self.paramschema = pd.DataFrame(columns=[param for param in PARAMSCHEMA.__annotations__.keys()], dtypes=[param for param in PARAMSCHEMA.__annotations__.values()])

        self.metaschema = pd.DataFrame(columns=[param for param in METASCHEMA.__annotations__.keys()], dtypes=[param for param in METASCHEMA.__annotations__.values()])
        self.schema = pd.DataFrame(columns=[param for param in SCHEMA.__annotations__.keys()], dtypes=[param for param in SCHEMA.__annotations__.values()])

        self.schema = pd.concat(self.param_schema, self.schema, verify_integrity=True)
        
        self.collectors = collectors

        self.data: pd.DataFrame = self.schema
        self.metadata: pd.DataFrame = self.metaschema

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
    def update_metadata(self, parameters: PARAMSCHEMA, info: METASCHEMA = None) -> pd.DataFrame:
        #retrieve existing metadata from parameters
        existing_metadata: pd.DataFrame = self.metadata.query(' & '.join([f'{key} == {value}' for key, value in parameters.items()]))

        if not existing_metadata:
            updated_metadata = self.default_metadata_initialize(parameters, info)
            return updated_metadata

        for key, value in {**parameters, **info}.items():
            updater = self.update_map.get(key, self.default_metadata_update)
            existing_metadata[key] = updater(value, existing_metadata[key])

        return existing_metadata
    
    def default_metadata_update(self, parameters: PARAMSCHEMA, info: METASCHEMA = None) -> pd.DataFrame:
        return pd.DataFrame({**parameters, **info})
    
    def default_metadata_initialize(self, parameters: PARAMSCHEMA, info: METASCHEMA = None,) -> pd.DataFrame:
        self.metadata[self.metadata.query(' & '.join([f'{key} == {value}' for key, value in parameters.items()]))] = info
        return self.metadata

    @abstractmethod
    def configure_update_map(self) -> dict[str, callable]:
        return {} #empty update map by default
    
    @abstractmethod
    def collect(self, parameters: PARAMSCHEMA, collectors: List[DataCollector]) -> pd.DataFrame:
        pass

    @abstractmethod
    def delete(self, parameters: PARAMSCHEMA, collectors: List[DataCollector]) -> pd.DataFrame:
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
