from sequential_loading.data_storage import SQLStorage
from sequential_loading.data_collector import DataCollector
from sequential_loading.data_processor import IntervalProcessor

from test_application.collectors import tiingoCollector

import datetime

from typedframe import TypedDataFrame, DATE_TIME_DTYPE
import pandas as pd

import numpy as np

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access the API_KEY environment variable
api_key = os.getenv('TIINGO_API_KEY')

#plz don't hack me
my_storage = SQLStorage("postgresql://bodszab@localhost:5432/xteststorage", createdb=True)

# Each datapoint has a corresponding set of parameters that are not necessarily unique.
# Each set of parameters has a corresponding set of metadata that is unique.

# So for example, our parameters are the ticker and the collector, since data 
# collected from different sources and corresponding to different tickers should be treated as their own units

# The metadata would be the domain and number of collected items corresponding to a particular collector and ticker

# The schema is the actual data that is collected for a given set of parameters

# Each DataProcessor will have its own Metaschema and a function defining how to update metadata during collection

# These are the parameters that characterize a unique set of datapoints


class EODParamSchema(TypedDataFrame):
    schema = {
        "ticker": str, 
        "collector": str
    }

class EODSchema(TypedDataFrame):
    schema = {
        "id": str,
        "date": DATE_TIME_DTYPE,
        "open": np.float64,
        "high": np.float64,
        "low": np.float64,
        "close": np.float64,
        "volume": np.float64
    }


tiingo_collector = tiingoCollector("TIINGO", api_key=api_key)

processor = IntervalProcessor("stock_processor", EODParamSchema, EODSchema, my_storage, unit="days")
processor.collect([tiingo_collector], ticker="AAPL", domain="/2021-01-01|2021-02-01")
# processor.delete([tiingo_collector], ticker="AAPL", domain="/2019-01-01|2020-01-01")