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
            'domain': lambda x, y, deletion = False: str(SparsityMappingString(unit=self.unit, string=x) - SparsityMappingString(unit=self.unit, string=y)) if deletion else str(SparsityMappingString(unit=self.unit, string=x) + SparsityMappingString(unit=self.unit, string=y)),
            'collected_items': lambda x, y, deletion: x - y if deletion else x + y
        }

    def initialize(self) -> None:
        self.storage.initialize(self.name, self.schema)
        self.storage.initialize(f"{self.name}_metadata", self.metaschema, primary_keys=(key for key in self.metaschema.schema.keys()))
    
    #this function is actually not unique to IntervalProcessor, but could be used by all DataProcessors
    def update_metadata(self, parameters: Type[TypedDataFrame], metadata: Type[TypedDataFrame], deletion: bool = False) -> None:
        metadata = pd.concat([parameters, metadata], axis=1)
        self.metaschema(metadata)

        #cache metadata to avoid unnecessary queries to storage
        if self.cached_metadata is None:
            self.cached_metadata = metadata
            return metadata
        
        parameter_query = self.format_query(**parameters.iloc[0].to_dict())  
        cached_metadata = self.cached_metadata.query(parameter_query) if self.cached_metadata is not None else None

        for key, value in self.update_map.items():
            cached_metadata[key] = value(cached_metadata.loc[0, key], metadata.loc[0, key], deletion)
        
        self.cached_metadata.loc[self.cached_metadata.eval(parameter_query)] = cached_metadata

        return cached_metadata

    #This is also not specific to IntervalProcessor, but could be used by all DataProcessors
    def update_data(self, parameters: Type[TypedDataFrame], data: Type[TypedDataFrame]) -> Type:  
        data = pd.concat([parameters, data], axis=1)
        self.schema(data)
        self.data = data

        return self.data

    def collect(self, collectors: List[DataCollector], domain:str = None, **parameters) -> pd.DataFrame:
        domain_sms = SparsityMappingString(unit=self.unit, string=domain)

        for collector in collectors:
            
            #get existing metadata
            parameter_query = self.format_query(collector=collector.name, **parameters)
            existing_domain = self.cached_metadata.query(parameter_query).iloc[0]['domain'] if self.cached_metadata is not None else None
            existing_domain = SparsityMappingString(unit=self.unit, string=existing_domain)

            #find domain to retrieve
            query_domain = domain_sms - existing_domain

            for interval, str_interval in zip(query_domain.get_intervals(), query_domain.get_str_intervals()):
                
                #retrieve data from collector
                data = collector.retrieve_data(interval=str_interval, resample_freq=self.unit, **parameters)
                if isinstance(data, str):
                    self.logger.error(f"Error retrieving data from collector {collector.name} for interval {interval} on parameters {parameters}: {data}")
                    continue
                
                #set metadata
                metadata = pd.DataFrame({
                    'domain': [f'/{str_interval[0]}|{str_interval[1]}'],
                    'collected_items': [len(data)]
                })
                
                #assign parameters for data and metadata
                data_params = pd.DataFrame({
                    "ticker": [parameters["ticker"] for _ in range(len(data))],
                    "collector": [collector.name for _ in range(len(data))]
                })
                metadata_params = pd.DataFrame({
                    "ticker": [parameters["ticker"]],
                    "collector": [collector.name]
                })
                metadata.collected_items = metadata.collected_items.astype(int)

                #updates, validates, and caches data and metadata
                data = self.update_data(data_params, data)
                metadata = self.update_metadata(metadata_params, metadata) 
                
                #store data
                self.storage.store_data(self.name, data, self.cached_metadata)
                

    def delete(self, collectors: List[DataCollector], domain: str, **parameters: Type[TypedDataFrame]) -> None:
        for collector in collectors:
            #query data with particular parameters
            parameter_query = self.format_query(**parameters)
            existing_domain = self.cached_metadata.query(parameter_query).iloc[0]['domain'] if self.cached_metadata is not None else None
            existing_domain = SparsityMappingString(unit=self.unit, string=existing_domain)
            #subtract existing domain from specified domain to get derrived domain, subtract specified domain from existing domain to get new domain
            query_domain = SparsityMappingString(unit=self.unit, string=domain)

            #loop over derrived domain and delete data
            for interval, str_interval in zip(query_domain.get_intervals(), query_domain.get_str_intervals()):
                #date / integer values should not need to be wrapped in quotes. This is a bug.
                deletion_query = f"{self.format_query(**parameters)} & date >= '{interval[0]}' & date <= '{interval[1]}'"

                deleted_count = self.storage.delete_data(self.name, query=deletion_query)


                metadata = pd.DataFrame({
                    'domain': [f'/{str_interval[0]}|{str_interval[1]}'],
                    'collected_items': [deleted_count]
                })
                metadata.collected_items = metadata.collected_items.astype(int)
                
                #assign parameters for data and metadata
                metadata_params = pd.DataFrame({
                    "ticker": [parameters["ticker"]],
                    "collector": [collector.name]
                })

                self.update_metadata(metadata_params, metadata, deletion=True)
                self.storage.store_data(self.name, None, self.cached_metadata)

        #update metadata with new domain and updated collected_items