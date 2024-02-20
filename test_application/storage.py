from sequential_loading.data_storage import DataStorage

from sqlalchemy import create_engine

class SQLStorage(DataStorage):
    _connections = {}

    def __init__(self, url, DataStorages=None):
        super().__init__(DataStorages)

    def __new__(cls, url, **kwargs):
        if url not in cls._connections:
            cls._connections[url] = super().__new__(cls)
            cls._connections[url].database_url = url
            cls._connections[url].engine = create_engine(url)

        return cls._connections[url]
    
    def create(self):
        for data_storage in self.DataStorages:
            data_storage.schema.tosql(self.engine, )
        
    def store(self):
        pass


from processors import stock_processor, weather_processor

WeatherStockLoader = SQLStorage(url="postgres://bruh", DataProcessors=[stock_processor, weather_processor])