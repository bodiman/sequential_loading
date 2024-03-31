from abc import ABC, abstractmethod

import pandas as pd
from typedframe import TypedDataFrame

from typing import List, Type


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

    def retrieve_data(self, processor_names: List[str], join_column: str = None, query: str = None, join_columns: List[str] = None, queries: List[str] = None, **kwargs) -> pd.DataFrame:
        assert not query or not queries, "Only query or queries can be specified"
        assert not join_column or not join_columns, "Only join_column or join_columns can be specified"

        assert join_column is not None or join_columns is not None or len(processor_names) <= 1, "At least one join column must be specified for multiple processors"

        if query:
            queries = [query] * len(processor_names)

        if join_column:
            join_columns = [join_column] * (len(processor_names) - 1)

        if not query and not queries:
            queries = [None] * len(processor_names)

        if not join_column and not join_columns:
            join_columns = [None] * (len(processor_names) - 1)
        
        full_table = self.retrieve_processor(processor_names[0], query=queries[0], **kwargs)

        for p_name, p_query, p_column in zip(processor_names[1:], queries[1:], join_columns):
            data = self.retrieve_processor(p_name, query=p_query, **kwargs)
            full_table = full_table.merge(data, on=p_column, how='left')

        return full_table

    #initialize a data processor
    @abstractmethod
    def initialize(self, name: str, data: Type[TypedDataFrame], **kwargs) -> None:
        pass

    @abstractmethod
    def delete_processor(self, name: str, **kwargs) -> None:
        pass
    
    @abstractmethod
    def store_data(self, name: str, data: pd.DataFrame, metadata: pd.DataFrame, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def delete_data(self, name: str, query: str, **kwargs) -> None:
        pass

    @abstractmethod
    def retrieve_processor(self, name: str, query: str = None, **kwargs) -> pd.DataFrame:
        pass


    
    # @abstractmethod
    # def load_sequential() -> pd.DataFrame:
    #     pass