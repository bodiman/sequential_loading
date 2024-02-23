from abc import ABC, abstractmethod

import pandas as pd


"""
Interface for storing and retrieving data.

Members
-------
DataProcessors: List[DataProcessor]
    List of data processors to be used for data storage.

Methods
-------

create: 
    Creates overhead for data storage.

store: (processor: DataProcessor, data: pd.DataFrame) -> pd.DataFrame:
    Stores data from processor.data and processor.metadata into storage.
"""
class DataStorage(ABC):
    def __init__(self, *DataProcessors) -> None:
        self.DataProcessors = DataProcessors

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def store(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
    
    # @abstractmethod
    # def load_sequential() -> pd.DataFrame:
    #     pass