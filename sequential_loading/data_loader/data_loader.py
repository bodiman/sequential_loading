from sequential_loading.data_storage import DataStorage

from abc import ABC, abstractmethod

import pandas as pd

from torch.utils.data import Dataset
import torch

"""
Dataloader is an interface for retrieving data from a storage object and packaging it into a pytorch dataset.
Needs to be able to 
"""
class DataLoader(ABC, Dataset):
    def __init__(self, storage: DataStorage, **parameters):
        self.storage = storage
        self.dataframe = self.load(**parameters)

    def __len__(self):
        return len(self.dataframe)

    def __getitem__(self, index):
        x = torch.tensor(self.dataframe.iloc[index].values)
        return x

    @abstractmethod
    def load(self, **parameters) -> pd.DataFrame:
        pass