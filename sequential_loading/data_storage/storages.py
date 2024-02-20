from sequential_loading.data_storage.data_storage import DataStorage

from sqlalchemy.orm import validates, declarative_base
from sqlalchemy_utils import database_exists, create_database

import pandas as pd

class SQLStorage(DataStorage):
    def __init__(self):
        self.Base = declarative_base

    def store(self) -> pd.DataFrame:
        pass

    def load_sequential() -> pd.DataFrame:
        pass