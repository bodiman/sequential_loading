from sequential_loading.data_storage.data_storage import DataStorage
from sequential_loading.data_collector import DataCollector
from sequential_loading.sparsity_mapping import SparsityMappingString

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

class IntervalParamSchema(TypedDict):
        ticker: str
        domain: str

class IntervalMetaSchema(TypedDict):
    id: str
    collector: str
    collected_items: int

class EODSchema(TypedDict):
    id: str
    date: datetime.datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

class IntervalProcessor(DataProcessor[IntervalParamSchema, IntervalMetaSchema, SCHEMA], Generic[SCHEMA]):

    def __init__(self, storage: DataStorage, collectors: List[DataCollector], unit: str) -> None:
        super().__init__(storage, collectors)

        self.unit = unit

        self.logger = logging.getLogger(__name__)

    def configure_update_map(self) -> dict[str, callable]:
        return {
            'domain': lambda x, y: SparsityMappingString(unit=self.unit, string=x) + SparsityMappingString(unit=self.unit, string=y),
            'collected_items': lambda x, y: x + y
        }

    def collect(self, collectors: List[DataCollector], **parameters: PARAMSCHEMA) -> pd.DataFrame:

        for collector in collectors:
            parameter_query = ' and '.join([f'{key} == {value}' for key, value in parameters.items()])
            existing_domain = self.metadata.query(parameter_query)
            existing_domain = SparsityMappingString(unit=self.unit, string=existing_domain)

            query_domain = parameters.domain - existing_domain

            for interval in query_domain.get_intervals():
                self.clear_data()

                # Collector must take interval argument to match schema
                data = collector.retrieve_data(**parameters)

                if not data:
                    self.logger.error(f"Failed to collect data from {collector.name} for {interval} with parameters {parameters}")
                    continue

                try:  
                    self.data = pd.concat(self.data, data, verify_integrity=True)

                except Exception as e:
                    self.logger.error(f"Collector {collector.name} returned improperly formatted dataframe for {interval} for parameters {parameters}")
                    continue
                
                meta_info = {
                    'id': uuid.uuid4(),
                    'collector': collector.name,
                    'collected_items': len(data)
                }

                self.update_metadata(parameters, meta_info)
                self.storage.store_data(self)


                

    def delete(self, collectors: List[DataCollector], **parameters: PARAMSCHEMA) -> None:
        for interval in parameters.domain.get_intervals():
            self.clear_data()
            for collector in collectors:
                parameter_query = ' & '.join([f'{key} == {value}' for key, value in parameters.items()])
                query = f"date >= {interval[0]} & date <= {interval[1]} & {parameter_query}"

                data = self.storage.retrieve_data(self, query=query)

                if not data:
                    self.logger.error(f"Failed to delete data from {collector.name} for {interval} with parameters {parameters}")
                    continue

                try:  
                    self.data = pd.concat(self.data, data, verify_integrity=True)

                except Exception as e:
                    self.logger.error(f"Collector {collector.name} returned improperly formatted dataframe for {interval} for parameters {parameters}")
                    continue

            self.storage.delete_data(self)


class EODSchema(TypedDict):
    id: str
    ticker: str
    date: datetime.datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

class EODMetaSchema(TypedDict):
    id: str
    ticker: str
    domain: str
