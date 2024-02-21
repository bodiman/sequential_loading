from sequential_loading.data_storage import DataStorage

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists

from typing import Type
import pandas as pd

import logging


class SQLStorage(DataStorage):
    _connections = {}

    def __init__(self, url: str, DataProcessors=None):
        super().__init__(DataProcessors)

        self.logger = logging.getLogger(__name__)

    def __new__(cls, url: str, **kwargs) -> DataStorage:
        if url not in cls._connections:
            cls._connections[url].engine = create_engine(url)
            cls._connections[url] = super().__new__(cls)
            cls._connections[url].database_url = url

        return cls._connections[url]
    
    def create(self):
        if database_exists(self.url):
            raise Exception(f"Database {self.url} already exists.")

        with self.engine.begin() as connection:
            try:
                self.logger.info("Creating Database")

                for data_processor in self.DataProcessors:
                    self.logger.info(f"Creating Table {data_processor.name}...")

                    #have to add some columns here.
                    metadata = pd.DataFrame(cols=["uuid", "time", *data_processor.parameters])
                    data_processor.schema.tosql(data_processor.name, con=connection, dtype=data_processor.dtypes, index=False)

                    self.logger.info(f"Created Table {data_processor.name}")

                self.logger.info("Done.")

            except Exception as e:
                connection.rollback()
                logging.error(f"Failed to create Database: {e}")

        
        
    def store(self, df: pd.DataFrame, table_name: str, overwrite:bool = False) -> bool:
        df.to_sql(table_name, con=self.engine, index=False)


from processors import stock_processor, weather_processor

WeatherStockLoader = SQLStorage(url="postgres://bruh", DataProcessors=[stock_processor, weather_processor])