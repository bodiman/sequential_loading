from sequential_loading.data_processor.data_processor import DataProcessor
from sequential_loading.data_storage.data_storage import DataStorage
from sequential_loading.data_collector import DataCollector

from sequential_loading.sparsity_mapping import SparsityMappingString
from typing import List, Type

import uuid
import datetime
import pandas as pd
from typedframe import TypedDataFrame

class IntervalMetaSchema(TypedDataFrame):
    schema = { 
        "domain": str,
        "collected_items": int
    }


class IntervalProcessor(DataProcessor):

    metaschema = IntervalMetaSchema

    def __init__(self, name: str, paramschema: Type[TypedDataFrame], schema: Type[TypedDataFrame], storage: DataStorage, unit: str, collectors: List[DataCollector] = None) -> None:
        super().__init__(name, paramschema, schema, self.metaschema, storage, collectors)

        self.unit = unit

        self.update_map = {
            'domain': lambda x, y: SparsityMappingString(unit=self.unit, string=x) + SparsityMappingString(unit=self.unit, string=y),
            'collected_items': lambda x, y: x + y
        }
    
    def update_metadata(self, parameters: Type[TypedDataFrame], metadata: Type[TypedDataFrame]) -> Type:
        metadata = self.metadata.query(parameters)

    def collect(self, collectors: List[DataCollector], domain:str = None, **parameters) -> pd.DataFrame:
        domain_sms = SparsityMappingString(unit=self.unit, string=domain)

        for collector in collectors:
            parameter_query = ' and '.join([f'{key} == {value}' for key, value in parameters.items()])

            existing_domain = self.metadata.query(parameter_query) if self.metadata else None
            existing_domain = SparsityMappingString(unit=self.unit, string=existing_domain)

            query_domain = domain_sms - existing_domain

            for interval in query_domain.get_intervals():
                # Collector must take interval argument to match schema
                data = collector.retrieve_data(domain=domain, resample_freq=self.unit, **parameters)

                if data is None:
                    self.logger.error(f"Failed to collect data from {collector.name} for {interval} with parameters {parameters}")
                    continue

                try:
                    #verify_integrity=True ensures that the parameters match the schema
                    print(data)
                    self.data = self.schema(data)

                except Exception as e:
                    print(e)
                    # self.logger.error(f"Collector {collector.name} returned improperly formatted dataframe for {interval} for parameters {parameters}")
                    continue
                
                metadata = {
                    'id': uuid.uuid4(),
                    'collector': collector.name,
                    'collected_items': len(data)
                }

                self.update_metadata(parameters, metadata)
                self.storage.store_data(self.name, self.data)
                self.storage.store_data(f"{self.name}_metadata", metadata)
                

    def delete(self, collectors: List[DataCollector], **parameters: Type[TypedDataFrame]) -> None:
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