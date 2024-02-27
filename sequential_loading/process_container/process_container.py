from sequential_loading.data_processor import DataProcessor
from sequential_loading.data_storage.data_storage import DataStorage

from abc import ABC, abstractmethod

from typing import List

"""
Container for DataProcessors under a shared Datastorage.

Members
-------
processes: List[DataProcessor]
    List of data processors.

storage: DataStorage
    Data storage for the container.

Methods
-------

retrieve_domain



"""
class ProcessContainer(ABC):
    def __init__(self, processes: List[DataProcessor], storage: DataStorage) -> None:
        self.processes = processes
        self.storage = storage