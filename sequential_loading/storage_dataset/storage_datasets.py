from sequential_loading.storage_dataset import StorageDataset
from sequential_loading.data_storage import DataStorage
from typing import List

class CachedDataset(StorageDataset):
    #need to get these parameters figured out
    def __init__(self, \
                 storage: DataStorage, \
                 processor_names: List[str], \
                 join_column: str | List[str] = None, \
                 query: str | List[str] = None, \
                 join_columns: List[str] | List[List[str]] = None, \
                 queries: List[str] | List[List[str]] = None, \
                 **parameters):
        super().__init__(storage, processor_names, join_column=join_column, query=query, join_columns=join_columns, queries=queries, **parameters)

    def load(self, **parameters):
        return self.storage.retrieve_data(self.processor_names, join_columns=self.join_columns, queries=self.queries, **parameters)