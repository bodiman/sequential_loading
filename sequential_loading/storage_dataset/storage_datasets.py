from sequential_loading.storage_dataset import StorageDataset
from sequential_loading.data_storage import DataStorage
from typing import List

import pandas as pd

import torch

class CachedDataset(StorageDataset):
    #need to get these parameters figured out
    def __init__(self, \
                 storage: DataStorage, \
                 processor_names: List[str], \
                 join_column: str | List[str] = None, \
                 query: str | List[str] = None, \
                 join_columns: List[str] | List[List[str]] = None, \
                 queries: List[str] | List[List[str]] = None, \
                 suffixes: List[str] = None, \
                 selected_columns=None, \
                 **parameters):
        super().__init__(storage, processor_names, join_column=join_column, query=query, join_columns=join_columns, queries=queries, suffixes=suffixes, selected_columns=selected_columns, **parameters)

    def load(self, **parameters) -> pd.DataFrame:
        return self.storage.retrieve_data(self.processor_names, join_columns=self.join_columns, queries=self.queries, suffixes=self.suffixes, **parameters)
    
    def __len__(self):
        return len(self.dataframe)

    def __getitem__(self, index):
        x = self.dataframe.iloc[index].values
        return torch.tensor(x, dtype=torch.float32)
    

class SparseIndexDataset(StorageDataset):
    #need to get these parameters figured out
    def __init__(self, \
                 storage: DataStorage, \
                 processor_names: List[str], \
                 window_size: int, \
                 index_column: str = None, \
                 join_column: str | List[str] = None, \
                 query: str | List[str] = None, \
                 join_columns: List[str] | List[List[str]] = None, \
                 queries: List[str] | List[List[str]] = None, \
                 suffixes: List[str] = None, \
                 selected_columns = None, \
                 **parameters):
        super().__init__(storage, processor_names, join_column=join_column, query=query, join_columns=join_columns, queries=queries, suffixes=suffixes, selected_columns=[*selected_columns, index_column], **parameters)
        self.index_column = self.dataframe[index_column]
        self.dataframe = self.dataframe.drop(columns=[index_column])
        self.window_size = window_size

        self.cached_indices = {}

        assert len(self.dataframe) > window_size, f"Window size {window_size} exceeds length of dataframe ({len(self.dataframe)} rows)."
        self.phases = len(self.dataframe) - window_size

    def load(self, **parameters) -> pd.DataFrame:
        return self.storage.retrieve_data(self.processor_names, join_columns=self.join_columns, queries=self.queries, suffixes=self.suffixes, **parameters)
    
    def __len__(self):
        return (2**self.window_size)*self.phases
    
    def __getitem__(self, index):
        if index in self.cached_indices:
            return self.cached_indices[index]

        phase = index // 2**self.window_size

        #don't want to return empty data, but may create bias towards previous datapoint as currently implemented
        mask = max(index % 2**self.window_size, 1)
        mask = [int(x) for x in bin(mask)[2:].zfill(self.window_size)]

        data = torch.tensor(self.dataframe.iloc[phase:phase+self.window_size].values)
        indices = self.index_column.iloc[phase:phase+self.window_size].values
        self.cached_indices[index] = (data, indices)

        return [d if m else None for d, m in zip(data, mask)], [i if m else None for i, m in zip(indices, mask)]
        