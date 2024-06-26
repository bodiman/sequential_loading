from sequential_loading.data_processor.data_processor import DataProcessor
from sequential_loading.data_storage.data_storage import DataStorage
from sequential_loading.data_collector import DataCollector

from sequential_loading.sparsity_mapping import SparsityMappingString
from typing import List, Type, Tuple

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

    def __init__(self, name: str, paramschema: Type[TypedDataFrame], schema: Type[TypedDataFrame], storage: DataStorage, unit: str, create_processor=False) -> None:
        super().__init__(name, paramschema, schema, self.metaschema, storage, create_processor)

        self.unit = unit

        self.update_map = {
            'domain': lambda x, y, deletion = False: str(SparsityMappingString(unit=self.unit, string=x) - SparsityMappingString(unit=self.unit, string=y)) if deletion else str(SparsityMappingString(unit=self.unit, string=x) + SparsityMappingString(unit=self.unit, string=y)),
            'collected_items': lambda x, y, deletion: x - y if deletion else x + y
        }

    def collect(self, domain:str = None, **parameters: dict) -> pd.DataFrame:
        #include collector as a parameter so that it can be contained in paramschema
        #could do this with domain as well, if it were a parameter
        collector = parameters["collector"]
        domain_sms = SparsityMappingString(unit=self.unit, string=domain)
            
        #get existing metadata
        existing_metadata, _ = self.retrieve_metadata(**parameters)
        existing_domain = existing_metadata['domain'] if existing_metadata is not None else None
        existing_domain = SparsityMappingString(unit=self.unit, string=existing_domain)

        #find domain to retrieve
        query_domain = domain_sms - existing_domain

        for interval, str_interval in zip(query_domain.get_intervals(), query_domain.get_str_intervals()):
            #retrieve data from collector
            data = collector.retrieve_data(interval=interval, resample_freq=self.unit, **parameters)
            if isinstance(data, str):
                self.logger.error(f"Error retrieving data from collector {collector.name} for interval {interval} on parameters {parameters}: {data}")
                continue
            
            #set metadata
            metadata = pd.DataFrame({
                'domain': [f'/{str_interval[0]}|{str_interval[1]}'],
                'collected_items': [len(data)]
            })
            metadata.collected_items = metadata.collected_items.astype(int)
            
            #assign parameters for data and metadata
            data_params = pd.DataFrame({
                k: [str(v) for _ in range(len(data))] for k, v in parameters.items()
            })

            metadata_params = pd.DataFrame({
                k: [str(v)] for k, v in parameters.items()
            })

            #updates, validates, and caches data and metadata
            if not data.empty:
                data = self.update_data(data_params, data)
            else:
                data = None
            
            metadata = self.update_metadata(metadata_params, metadata) 
            
            #store data
            #update_data and metadata return new data. But, metadata replaces the old metadata, so we need to write the whole cached metadata.
            #maybe this can be changed in the future
            self.storage.store_data(self.name, data, self.cached_metadata)
                

    def delete(self, domain: str, **parameters: Type[TypedDataFrame]) -> None:
        # collector = parameters["collector"]
        #query data with particular parameters
        existing_metadata, _ = self.retrieve_metadata(**parameters)
        existing_domain = existing_metadata['domain'] if existing_metadata is not None else None
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
                k: [str(v)] for k, v in parameters.items()
            })

            self.update_metadata(metadata_params, metadata, deletion=True)
            self.storage.store_data(self.name, None, self.cached_metadata)

        #update metadata with new domain and updated collected_items