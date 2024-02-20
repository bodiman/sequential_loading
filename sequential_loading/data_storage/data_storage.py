from abc import ABC, abstractmethod

import pandas as pd

class DataStorage(ABC):
    def __init__(self, *DataProcessors) -> None:
        self.DataProcessors = DataProcessors

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def store(self, data: pd.DataFrame) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def load_sequential() -> pd.DataFrame:
        pass