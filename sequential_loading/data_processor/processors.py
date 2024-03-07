from sequential_loading.data_processor.data_processor import DataProcessor, SCHEMA
from sequential_loading.data_storage.data_storage import DataStorage
from sequential_loading.data_collector import DataCollector

from sequential_loading.sparsity_mapping import SparsityMappingString
from typing import List, TypedDict, Generic

import uuid
import datetime
import pandas as pd

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

    def configure_update_map(self) -> dict[str, callable]:
        return {
            'domain': lambda x, y: SparsityMappingString(unit=self.unit, string=x) + SparsityMappingString(unit=self.unit, string=y),
            'collected_items': lambda x, y: x + y
        }

    def collect(self, collectors: List[DataCollector], **parameters: IntervalParamSchema) -> pd.DataFrame:

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
                

    def delete(self, collectors: List[DataCollector], **parameters: IntervalParamSchema) -> None:
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