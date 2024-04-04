from sequential_loading.data_collector import DataCollector
from sequential_loading.sparsity_mapping import SparsityMappingString

from test_application.schemas import EODSchema, WeatherSchema

import httpx
import pandas as pd

from io import StringIO

import uuid

from typing import Tuple

import datetime

import pandas as pd

#EOD Collectors
    
class tiingoCollector(DataCollector):

    def __init__(self, api_key=None):
        super().__init__(name="TIINGO", schema=EODSchema)

        self.api_key = api_key
        self.resample_map = {
            "days": "daily",
            "months": "monthly",
            "years": "annually"
        }
        

    def retrieve_data(self, interval: Tuple[str, str], ticker, resample_freq=None, **kwargs):
        interval = (interval[0].strftime("%Y-%m-%d"), interval[1].strftime("%Y-%m-%d"))

        if resample_freq is None:
            resample_freq = self.resample_map["days"]

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
    
class newYorkWeatherCollector(DataCollector):
    def __init__(self):
        super().__init__(name="NEWYORKCOLLECTOR", schema=WeatherSchema)

        self.data = pd.read_csv("test_application/nyc_temperature.csv")
        print(self.data)

    def retrieve_data(self, interval: Tuple[str, str], location="New York", **kwargs):

        if location != "New York":
            return f"Location {location} not supported."

        startdate = interval[0].strftime("%d-%m-%y")
        enddate = interval[1].strftime("%d-%m-%y")

        result = self.data[(self.data['date'] >= startdate) & (self.data['date'] <= enddate)]

        result = result[['date', 'tmax', 'tmin', 'tavg', 'CDD', 'precipitation', 'new_snow']]
        result['new_snow'][result['new_snow'] != 0] = 1
        result['precipitation'][result['precipitation'] != 0] = 1       
        
        return result