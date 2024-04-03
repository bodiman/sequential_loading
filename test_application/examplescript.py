from sequential_loading.data_storage import SQLStorage
from sequential_loading.data_processor import IntervalProcessor
from sequential_loading.storage_dataset import CachedDataset
from test_application.collectors import tiingoCollector

from sequential_loading.data_typing import LoaderSchema

from test_application.schemas import EODParamSchema, EODSchema

import datetime

from typedframe import TypedDataFrame, DATE_TIME_DTYPE
import pandas as pd

import numpy as np

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access the API_KEY environment variable
tiingo_api_key = os.getenv('TIINGO_API_KEY')
weather_api_key = os.getenv('CLIMATE_API_KEY')

#plz don't hack me
my_storage = SQLStorage("postgresql://bodszab@localhost:5432/xteststorage")

# Each datapoint has a corresponding set of parameters that are not necessarily unique.
# Each set of parameters has a corresponding set of metadata that is unique.

# So for example, our parameters are the ticker and the collector, since data 
# collected from different sources and corresponding to different tickers should be treated as their own units

# The metadata would be the domain and number of collected items corresponding to a particular collector and ticker

# The schema is the actual data that is collected for a given set of parameters

# Each DataProcessor will have its own Metaschema and a function defining how to update metadata during collection

# These are the parameters that characterize a unique set of datapoints


tiingo_collector = tiingoCollector(api_key=tiingo_api_key)

# my_storage.delete_processor("stock_processor")
stock_processor = IntervalProcessor("stock_processor", EODParamSchema, EODSchema, my_storage, unit="days", create_processor=True)
# stock_processor.collect([tiingo_collector], ticker="SPY", domain="/2020-01-01|2022-02-01")
# stock_processor.collect([tiingo_collector], ticker="QQQ", domain="/2020-01-01|2022-02-01")
# stock_processor.delete([tiingo_collector], ticker="QQQ", domain="/2021-02-02|2022-02-01")

# processor_list = ["stock_processor", "stock_processor"]
# queries = ["ticker == 'QQQ'", "ticker == 'SPY'"]
# selected_columns = ["open", "high", "low", "close", "volume"]

# dataset = CachedDataset(my_storage, processor_names=processor_list, join_column=["date"], selected_columns=selected_columns, queries=queries)

# print(dataset[0])