from sequential_loading.storage_dataset import StorageDataset

class SimpleCachedDataset(StorageDataset):
    def __init__(self, storage, **parameters):
        super().__init__(storage, **parameters)

    def load(self, **parameters):
        return self.storage.retrieve_data(**parameters)