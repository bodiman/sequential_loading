from sequential_loading.data_collector import DataCollector
from sequential_loading.sparsity_mapping import SparsityMappingString

import httpx
import pandas as pd

from io import StringIO

import uuid

from typing import Tuple

#EOD Collectors

# class tiingoCollector(DataCollector):

#     def retrieve_data(self, domain, ticker=None):
#         result = httpx.get(f"https://tiingo_url/{ticker}/{domain}")
#         return result
    
class tiingoCollector(DataCollector):

    def __init__(self, name, api_key):
        super().__init__(name)
        self.api_key = api_key
        self.resample_map = {
            "day": "daily",
            "month": "monthly",
            "year": "annually"
        }
        

    def retrieve_data(self, interval: Tuple[str, str], ticker, resample_freq=None):
        if resample_freq is None:
            resample_freq = self.resample_map["day"]

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Token {self.api_key}'
        }

        response = httpx.get(f"https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={interval[0]}&endDate={interval[1]}&resampleFreq={self.resample_map[resample_freq]}&format=csv", headers=headers)
        if response.is_error or "Error" in response.text:
            return f'Failed to retrieve data for {ticker} with the following response: "{response.text}".'

        df = pd.read_csv(StringIO(response.text), sep=",")

        #add id column
        df['id'] = [uuid.uuid4() for _ in range(len(df))]
        df = df[['id', 'date', 'open', 'high', 'low', 'close', 'volume']]

        #convert datatypes
        df.date = pd.to_datetime(df.date)
        df.volume = df.volume.astype(float)

        return df
    

# Weather Collectors
    
class openWeatherCollector(DataCollector):

    def retrieve_data(self, domain, location=None):
        result = httpx.get(f"openweather_url/{location}/{domain}")
        return result