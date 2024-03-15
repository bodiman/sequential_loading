from sequential_loading.data_collector import DataCollector
from sequential_loading.sparsity_mapping import SparsityMappingString

import httpx
import pandas as pd

from io import StringIO

import uuid

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
        

    def retrieve_data(self, domain, ticker, resample_freq=None):
        if resample_freq is None:
            resample_freq = self.resample_map["day"]

        domain = SparsityMappingString(unit=resample_freq, string=domain)
        intervals = domain.get_str_intervals()[0]

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Token {self.api_key}'
        }

        response = httpx.get(f"https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={intervals[0]}&endDate={intervals[1]}&resampleFreq={self.resample_map[resample_freq]}&format=csv", headers=headers)
        if response.is_error or "Error" in response.text:
            return f'Failed to retrieve data for {ticker} with the following response: "{response.text}".'

        df = pd.read_csv(StringIO(response.text), sep=",")

        df = df.rename(columns={
            'date': 'date',
            'adjVolume': 'volume',
            'adjClose': 'close',
            'adjHigh': 'high',
            'adjLow': 'low',
            'adjOpen': 'open',
        })
        df['ticker'] = ticker
        df['resample_freq'] = resample_freq
        df['id'] = [uuid.uuid4() for _ in range(len(df))]
        df = df[['id', 'date', 'open', 'high', 'low', 'close', 'volume']]

        return df
    

# Weather Collectors
    
class openWeatherCollector(DataCollector):

    def retrieve_data(self, domain, location=None):
        result = httpx.get(f"openweather_url/{location}/{domain}")
        return result