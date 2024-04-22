from sequential_loading.data_storage import SQLStorage
from sequential_loading.data_processor import IntervalProcessor
from sequential_loading.storage_dataset import CachedDataset
from test_application.collectors import tiingoCollector, newYorkWeatherCollector

from sequential_loading.data_typing import LoaderSchema

from test_application.schemas import EODParamSchema, EODSchema, WeatherParamSchema, WeatherSchema

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

import os



# Access the API_KEY environment variable
tiingo_api_key = os.getenv('TIINGO_API_KEY')
weather_api_key = os.getenv('CLIMATE_API_KEY')

#plz don't hack me
my_storage = SQLStorage("postgresql://bodszab@localhost:5432/xteststorage", create_storage=True)

# weather_processor = IntervalProcessor("weather_processor", WeatherParamSchema, WeatherSchema, my_storage, unit="days", create_processor=True)

# weather_collector = newYorkWeatherCollector()
# weather_processor.collect(collector=weather_collector, location="New York", domain="/2019-01-01|2020-01-01")

stock_processor = IntervalProcessor("stock_processor", EODParamSchema, EODSchema, my_storage, unit="days", create_processor=True)

tiingo_collector = tiingoCollector(api_key=tiingo_api_key)
# stock_processor.collect(collector=tiingo_collector, ticker="SPY", domain="/2019-01-01|2022-02-01")
stock_processor.delete(collector=tiingo_collector, ticker="SPY", domain="/2020-01-01|2021-01-01")
# stock_processor.collect(collector=tiingo_collector, ticker="SPY", domain="/2020-01-01|2021-01-01")


# processor_list = ["stock_processor", "stock_processor"]
# queries = ["ticker=='SPY'", "ticker=='SPY'"]
# selected_columns = ["open", "high", "low", "close", "volume"]

# processor_list = ["stock_processor", "weather_processor"]
# queries = ["ticker == 'SPY'", "location == 'New York'"]
# selected_columns = ["open", "high", "low", "close", "volume", "tmin", "tmax", "tavg", "cdd", "precipitation", "new_snow"]
# dataset = CachedDataset(my_storage, processor_names=processor_list, join_column=["date"], selected_columns=selected_columns, queries=queries)

# print(len(dataset))
# print(dataset[0])