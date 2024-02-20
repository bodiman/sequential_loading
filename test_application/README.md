# Test application for the sequential_loading library

In this example, we would like to use the sequential_loading interface to produce a training dataset for predicting stock market EOD performance based on the previous days' performance and the weather forecast for today.

# # Creating the storage

We start by defining a storage mechanism. The DataStorage class is responsible for taking the data and metadata synthesizes by its DataProcessors and storing them so they can be loaded sequentially. There are predefined storages that can be loaded from sequential_loading.data_storage, but let's define our own. For this project, we would like to store our data in a Postgres database.

```
from sequential_loading.data_storage import DataStorage

class SQLStorage(DataStorage):
    _connections = {}

    def __init__(self, url, DataStorages=None):
        super().__init__(DataStorages)

    def __new__(cls, url, **kwargs) 
        if url not in cls._connections:
            cls._connections[url] = super().__new__(cls)
            cls._connections[url].engine = create_engine(self.database_url)

        return cls._connections[url]
        
    def store(self):
        pass
```