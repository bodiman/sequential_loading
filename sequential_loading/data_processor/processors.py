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
    
    def update_metadata(self, parameters: Type[TypedDataFrame], metadata: Type[TypedDataFrame]) -> None:
        #cache metadata to avoid unnecessary queries to storage
        if self.cached_metadata is None:
            self.cached_metadata = metadata
            return
        
        parameter_query = self.format_query(**parameters)
        cached_metadata = self.metadata.query(parameter_query) if self.metadata else None
        
        for key, value in self.update_map.items():
            cached_metadata[key] = value(cached_metadata[key], metadata[key])
        
        self.cached_metadata.loc[self.cached_metadata.eval(parameter_query)] = cached_metadata

    def update_data(self, parameters: Type[TypedDataFrame], data: Type[TypedDataFrame]) -> Type:        
        data = pd.concat([self.data, data], verify_integrity=True)
        data = self.schema(data)

        self.data = data

    def collect(self, collectors: List[DataCollector], domain:str = None, **parameters) -> pd.DataFrame:
        domain_sms = SparsityMappingString(unit=self.unit, string=domain)

        for collector in collectors:
            parameter_query = self.format_query(**parameters)
            existing_domain = self.metadata.query(parameter_query)['domain'] if self.metadata else None

            existing_domain = SparsityMappingString(unit=self.unit, string=existing_domain)

            query_domain = domain_sms - existing_domain

            for interval, str_interval in zip(query_domain.get_intervals(), query_domain.get_str_intervals()):
                # Collector must take interval argument to match schema
                data = collector.retrieve_data(interval=str_interval, resample_freq=self.unit, **parameters)

                try:
                    #set parameters for data
                    param_update = pd.DataFrame({
                        "ticker": [parameters["ticker"] for _ in range(len(data))],
                        "collector": [collector.name for _ in range(len(data))]
                    })

                    data = pd.concat([data, param_update], axis=1)

                    metadata = pd.DataFrame({
                        'collector': [collector.name],
                        'collected_items': [len(data)]
                    })
                    
                    print(data) 
                    print(metadata)

                    #validate against schema
                    data = self.schema(data)

                    #breaking here currently
                    metadata = self.metaschema(metadata)

                    self.update_metadata(parameters, metadata)
                    self.update_data(parameters, data)

                    self.storage.store_data(self.name, self.data)
                    self.storage.store_data(f"{self.name}_metadata", metadata)         

                except Exception as e:
                    self.logger.error(f"Error retrieving data from collector {collector.name} for interval {interval} on parameters {parameters}: {e}")
                

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