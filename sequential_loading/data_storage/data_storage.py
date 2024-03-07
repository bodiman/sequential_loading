from abc import ABC, abstractmethod

import pandas as pd

from typing import List


"""
Interface for storing and retrieving data.

Members
-------
None

Methods
-------

initialize: (*DataProcessors: List[DataProcessor]) -> None 
    Creates data and metadata storage objects for each DataProcessor.

store: (processor: DataProcessor) -> pd.DataFrame:
    Stores data from processor.data and processor.metadata into storage.

retrieve: (processor: DataProcessor) -> pd.DataFrame:
    Retrieves data from storage into processor.data and processor.metadata based on specified conditions.

delete_rows: (processor: DataProcessor, ids: List[str]) -> None:
    Deletes rows from storage based on specified ids.

delete_processor: (processor: DataProcessor) -> None:
    Deletes data and metadata storage objects for a particular DataProcessor.

"""
class DataStorage(ABC):
    @abstractmethod
    def initialize(self, name: str, data: pd.DataFrame, metadata: pd.DataFrame, **kwargs) -> None:
        pass
    
    @abstractmethod
    def store_data(self, name: str, data: pd.DataFrame, metadata: pd.DataFrame, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def retrieve_data(self, name: str, conditions: object = None, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def retrieve_metadata(self, name: str, conditions: object = None, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def delete_rows(self, name: str, ids: List[str], **kwargs) -> None:
        pass

    @abstractmethod
    def delete_processor(self, name: str, **kwargs) -> None:
        pass


    
    # @abstractmethod
    # def load_sequential() -> pd.DataFrame:
    #     pass