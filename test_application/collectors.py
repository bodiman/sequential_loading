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
    
def reformat_datestr(datestr):
    datestr = datestr.split("/")

    #day, month, year to year, month, day
    datestr = datestr[::-1]
    return "-".join(datestr)
    
class newYorkWeatherCollector(DataCollector):
    def __init__(self):
        super().__init__(name="NEWYORKCOLLECTOR", schema=WeatherSchema)

        self.data = pd.read_csv("test_application/nyc_temperature.csv")

    def retrieve_data(self, interval: Tuple[str, str], location="New York", **kwargs):

        if location != "New York":
            return f"Location {location} not supported."

        startdate = interval[0].strftime("%y-%m-%d")
        enddate = interval[1].strftime("%y-%m-%d")

        self.data["date"] = self.data["date"].apply(reformat_datestr)
        result = self.data[(self.data['date'] >= startdate) & (self.data['date'] <= enddate)]
        result['date'] = pd.to_datetime(result['date'], format="%y-%m-%d")

        result = result[['date', 'tmax', 'tmin', 'tavg', 'CDD', 'precipitation', 'new_snow']]

        #chained assignment warning, need to fix
        result['new_snow'][result['new_snow'] != "0"] = 1
        result['precipitation'][result['precipitation'] != "0"] = 1  

        result['precipitation'] = result['precipitation'].astype(float)
        result['new_snow'] = result['new_snow'].astype(float)
        result['CDD'] = result['CDD'].astype(float)

        result = result.rename(columns={'CDD': 'cdd'})

        # print("this ran 10")
        # print(result)

        result['id'] = [uuid.uuid4() for _ in range(len(result))]
        
        return result