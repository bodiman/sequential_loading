from sequential_loading.data_storage.data_storage import DataStorage
from sequential_loading.data_collector import DataCollector
from sequential_loading.sparsity_mapping import SparsityMapping

from abc import ABC, abstractmethod

import pandas as pd

from typing import List, Type, TypedDict, TypeVar, Generic

import logging

import datetime

SCHEMA = TypeVar("SCHEMA")
PARAMSCHEMA = TypeVar("PARAMSCHEMA")
METASCHEMA = TypeVar("METASCHEMA")


class DataProcessor(ABC, Generic[PARAMSCHEMA, METASCHEMA, SCHEMA]):
    def __init__(self, collectors: List[DataCollector]):
        #convert types into dataframes (schemas)
        self.paramschema = pd.DataFrame(columns=[param for param in PARAMSCHEMA.__annotations__.keys()], dtypes=[param for param in PARAMSCHEMA.__annotations__.values()])

        self.metaschema = pd.DataFrame(columns=[param for param in METASCHEMA.__annotations__.keys()], dtypes=[param for param in METASCHEMA.__annotations__.values()])
        self.schema = pd.DataFrame(columns=[param for param in SCHEMA.__annotations__.keys()], dtypes=[param for param in SCHEMA.__annotations__.values()])

        self.schema = pd.concat(self.param_schema, self.schema, verify_integrity=True)
        
        self.collectors = collectors

    #function for updating metadata. Takes in 
    @abstractmethod
    def update_metadata(self, **parameters) -> None:
        return pd.DataFrame([dict(parameters)])



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
        collector: str
        ticker: str
        domain: str

class IntervalMetaSchema(TypedDict):
    id: str
    collector: str
    ticker: str
    domain: str

class EODSchema(TypedDict):
    id: str
    date: datetime.datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

class IntervalProcessor(DataProcessor[IntervalParamSchema, IntervalMetaSchema], SCHEMA):

    param_type = IntervalParamSchema


    def __init__(self, schema: Type, storage: DataStorage, collectors: List[DataCollector], unit: str) -> None:
        super().__init__(self.param_schema, schema, collectors)

        self.data: pd.DataFrame = self.schema
        self.metadata: pd.DataFrame = self.metaschema

        self.storage: DataStorage = storage
        self.storage.initialize(self)

        metadata = self.storage.retrieve_metadata(self)
        self.metadata = pd.concat(self.metadata, metadata, verify_integrity=True)

        self.unit = unit

        self.logger = logging.getLogger(__name__)

    def clear_data(self) -> None:
        self.data = self.schema

    def collect_all(self, domain: SparsityMapping, **parameters: dict[str, str]) -> pd.DataFrame:
        self.collect(self.collectors, domain, **parameters)

    def delete_all(self, domain: SparsityMapping, **parameters) -> None:
        self.delete(domain, self.collectors, parameters)

    def collect(self, collectors: List[DataCollector], domain: SparsityMapping, **parameters: dict[str, str]) -> pd.DataFrame:

        for collector in collectors:
            parameter_query = ' and '.join([f'{key} == {value}' for key, value in parameters.items()])
            existing_domain = self.metadata.query(parameter_query)['domain']
            existing_domain = SparsityMapping(unit=self.unit, string=existing_domain)

            query_domain = domain - existing_domain

            for interval in query_domain.get_intervals():
                self.clear_data()

                # Collector must take interval argument to match schema
                data = collector.retrieve_data(interval=interval, **parameters)

                if not data:
                    self.logger.error(f"Failed to collect data from {collector.name} for {interval} with parameters {parameters}")
                    continue

                try:  
                    self.data = pd.concat(self.data, data, verify_integrity=True)

                except Exception as e:
                    self.logger.error(f"Collector {collector.name} returned improperly formatted dataframe for {interval} for parameters {parameters}")
                    continue
                
                
                sparsity_interval = SparsityMapping(unit=self.unit, string=f"/{existing_domain.date_to_str(interval[0])}|{existing_domain.date_to_str(interval[1])}")
                existing_domain = existing_domain + sparsity_interval

                self.metadata.query(parameter_query)['domain'] = existing_domain.string

                self.storage.store_data(self)


                

    def delete(self, domain: SparsityMapping, collectors: List[DataCollector], **parameters: dict[str, str]) -> None:
        for interval in domain.get_intervals():
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
