from sequential_loading.data_storage import DataStorage
from sequential_loading.data_processor import DataProcessor

from sqlalchemy import create_engine, delete, MetaData, Table
from sqlalchemy_utils import database_exists

import functools

from typing import Type, List
import pandas as pd
import datetime

import logging

def dbsafe(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with self.engine.begin() as connection:
            try:
                func(*args, **kwargs, connection=connection)
            except Exception as e:
                connection.rollback()
                logging.error(f"Failed to create Database: {e}")

    return wrapper


class SQLStorage(DataStorage):
    _connections = {}


    def __init__(self, url: str, DataProcessors: List[DataProcessor]):
        super().__init__(DataProcessors)

        self.database_url = url
        self.metadata = MetaData(bind=self.engine)
        self.metadata.reflect()

        self.logger = logging.getLogger(__name__)

    def __new__(cls, url: str, **kwargs) -> DataStorage:
        if url not in cls._connections:
            cls._connections[url].engine = create_engine(url)
            cls._connections[url] = super().__new__(cls)

        return cls._connections[url]
    
    @dbsafe
    def create(self, connection=None):
        if database_exists(self.url):
            raise Exception(f"Database {self.url} already exists.")

        self.logger.info("Creating Database")

        for data_processor in self.DataProcessors:
            self.logger.info(f"Creating Table {data_processor.name}...")

            data_processor.schema.tosql(data_processor.name, con=connection, dtype=data_processor.dtypes, index=False)
            data_processor.metadata_schema.to_sql(f"{data_processor.name}_metadata")

            self.logger.info(f"Created Table {data_processor.name}")

        self.logger.info("Done.")

        
    @dbsafe  
    def store(self, processor: DataProcessor, connection=None) -> None:
        processor.data.to_sql(processor.name, con=self.engine, index=False)
        processor.metadata.to_sql(f"{processor.name}_metadata", con=self.engine, index=False)

    @dbsafe
    def retrieve(self, processor: DataProcessor, connection=None) -> pd.DataFrame:
        pass

    @dbsafe
    def delete(self, processor: DataProcessor, uuids: List[str], connection=None) -> None:
        table = self.metadata.tables.get(processor.name)
        delete_statement = delete(table).where(table.c.uuid.in_(uuids))

        connection.execute(delete_statement)


from processors import stock_processor, weather_processor

WeatherStockLoader = SQLStorage(url="postgres://bruh", DataProcessors=[stock_processor, weather_processor])