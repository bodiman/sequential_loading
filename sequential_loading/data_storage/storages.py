from sequential_loading.data_storage import DataStorage
from sequential_loading.data_processor import DataProcessor

from sqlalchemy import create_engine, delete, MetaData, Table, text, inspect
from sqlalchemy.engine.url import make_url

from sqlalchemy_utils import database_exists, create_database

import functools

from typing import Type, List, Union
import pandas as pd
import numpy as np
from typedframe import TypedDataFrame, DATE_TIME_DTYPE
import datetime

import logging

def dbsafe(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with self.engine.begin() as connection:
            try:
                return func(self, *args, **kwargs, connection=connection)
            except Exception as e:
                connection.rollback()
                raise Exception(e)

    return wrapper


class SQLStorage(DataStorage):
    _connections = {}

    type_mapping = {
        int: 'INTEGER',
        float: 'REAL',
        str: 'TEXT',
        bool: 'INTEGER',  # SQLite uses 0 and 1 for boolean values
        np.float64: 'REAL',
        np.int64: 'INTEGER',
        DATE_TIME_DTYPE: 'TIMESTAMP'
    }


    def __init__(self, url: str, create_storage: bool = False):
        super().__init__()

        self.database_url = make_url(url)
        self.name = self.database_url.database
        self.metadata = MetaData()

        if not database_exists(url):
            if create_storage:
                create_database(url)
            else:
                raise Exception(f"A database named {self.name} does not exist. To create one, set create_storage=True.")

        self.inspector = inspect(self.engine)
        self.metadata.reflect(bind=self.engine)

        self.tables = self.inspector.get_table_names()

        self.processors = {}

        self.logger = logging.getLogger(__name__)

    def __new__(cls, url: str, **kwargs) -> DataStorage:
        if url not in cls._connections:
            cls._connections[url] = super().__new__(cls)
            cls._connections[url].engine = create_engine(url)

        return cls._connections[url]
    
    def process_query(self, query: str, default_val: str = "TRUE") -> str:
        conditions = " AND ".join(query.split("&")) if query else default_val
        conditions = conditions.replace("===", "=").replace("==", "=")
        conditions = conditions.replace('"', "'")
        conditions = text(conditions)

        return conditions
    
    @dbsafe
    def create_table(self, name: str, tableschema: Type[TypedDataFrame], primary_keys: tuple[str] = None, connection=None):
        columns = ', '.join(f'{name} {self.type_mapping[dtype]}' for name, dtype in tableschema.schema.items())
        primary_keys_string = ', PRIMARY KEY (' + ', '.join(primary_keys) + ')' if primary_keys else ''

        query = text(f'CREATE TABLE {name} ({columns} {primary_keys_string})')
        connection.execute(query)
    
    @dbsafe
    def initialize(self, name: str, tableschema: Type[TypedDataFrame], primary_keys: tuple[str] = None, create_processor: bool=False, connection=None):
        if not self.inspector.has_table(name):
            if create_processor:
                self.logger.info(f"Creating Table {name}...")

                self.create_table(name, tableschema, primary_keys=primary_keys)

                self.logger.info(f"Created Table {name}")
            else:
                raise Exception(f"Table {name} does not exist. To create one, set create_processor=True.")

        self.processors[name] = Table(name, self.metadata, autoload_with=self.engine)

        
    @dbsafe  
    def store_data(self, name:str, data: pd.DataFrame = None, metadata: pd.DataFrame = None, connection=None) -> None:
        if data is not None:
            data.to_sql(name, con=self.engine, if_exists="append", index=False)
        
        if metadata is not None:
            metadata.to_sql(f"{name}_metadata", con=self.engine, if_exists="replace", index=False)

    @dbsafe
    def retrieve_processor(self, name: str, query: str = None, connection=None) -> pd.DataFrame:
        table = self.metadata.tables.get(name)
        
        conditions = self.process_query(query)

        select_statement = table.select().where(conditions)

        data = connection.execute(select_statement).fetchall()

        if data is None:
            return None

        return pd.DataFrame(data)
    
    @dbsafe
    def delete_data(self, name: str, query: str = None, connection=None) -> None:
        table = self.metadata.tables.get(name)
        
        conditions = self.process_query(query, "FALSE")

        delete_statement = delete(table).where(conditions)

        result = connection.execute(delete_statement)

        return result.rowcount

    @dbsafe
    def delete_processor(self, name: str, connection=None) -> None:
        query = text(f"DROP TABLE IF EXISTS {name}")
        connection.execute(query)

        query = text(f"DROP TABLE IF EXISTS {name}_metadata")
        connection.execute(query)

        connection.commit()
        connection.close()

        if name in self.processors:
            self.processors.pop(name)