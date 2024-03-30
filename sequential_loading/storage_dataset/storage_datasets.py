from sequential_loading.storage_dataset import StorageDataset

class CachedDataset(StorageDataset):
    #need to get these parameters figured out
    def __init__(self, storage, **parameters):
        super().__init__(storage, **parameters)

    def load(self, **parameters):
        return self.storage.retrieve_data(**parameters)