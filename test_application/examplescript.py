from sequential_loading.data_storage import SQLStorage
from sequential_loading.data_collector import DataCollector
from sequential_loading.data_processor import IntervalProcessor

#plz don't hack me
my_storage = SQLStorage("postgres://bodszab@localhost:5432/teststorage")