from sequential_loading.data_storage import DataStorage

from abc import ABC, abstractmethod

import pandas as pd

from torch.utils.data import Dataset
import torch

from typing import List

"""
Dataloader is an interface for retrieving data from a storage object and packaging it into a pytorch dataset.
Needs to be able to 
"""
class StorageDataset(ABC, Dataset):
    def __init__(self, storage: DataStorage, \
                 processor_names: List[str], \
                 join_column: str | List[str] = None, \
                 join_columns: List[str] | List[List[str]] = None, \
                 query: str | List[str] = None, \
                 queries: List[str] | List[List[str]] = None, \
                 selected_columns: List[str] = None, \
                 **parameters):
        
        assert not query or not queries, "Only query or queries can be specified"
        assert not join_column or not join_columns, "Only join_column or join_columns can be specified"
        assert join_column is not None or join_columns is not None or len(processor_names) <= 1, "At least one join column must be specified for multiple processors"

        if query:
            queries = [query] * len(processor_names)

        if join_column:
            join_columns = [join_column] * (len(processor_names) - 1)

        self.processor_names = processor_names
        self.queries = queries
        self.join_columns = join_columns

        self.storage = storage
        self.dataframe = self.load(**parameters)

        if selected_columns is not None:
            self.dataframe = self.dataframe.filter(regex=f"({'|'.join(selected_columns)})")

    def __len__(self):
        return len(self.dataframe)

    def __getitem__(self, index):
        x = self.dataframe.iloc[index].values
        return torch.tensor(x)

    @abstractmethod
    def load(self, **parameters) -> pd.DataFrame:
        pass