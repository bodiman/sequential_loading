from abc import ABC, abstractmethod
import pandas as pd
from typedframe import TypedDataFrame

class DataCollector(ABC):
    def __init__(self, name, schema):
        self.name = name
        self.schema = schema

    @abstractmethod
    def retrieve_data(self, **parameters) -> pd.DataFrame | str:
        pass