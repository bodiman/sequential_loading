from abc import ABC, abstractmethod
import pandas as pd
from typedframe import TypedDataFrame

class DataCollector(ABC):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def retrieve_data(self, **parameters) -> pd.DataFrame | str:
        pass