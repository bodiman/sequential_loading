from sequential_loading.data_processor import DataProcessor
from test_application.collectors import tiingoCollector, robinhoodCollector

from typing import List
import pandas as pd


class EODProcessor(DataProcessor):
    parameters: List[str] = ["ticker"]
    schema: pd.DataFrame = pd.DataFrame(columns=[""])

    def __init__(self, collectors):
        pass

stock_processor = EODProcessor(tiingoCollector, robinhoodCollector)


class WeatherProcessor(DataProcessor):
    parameters: List[str] = ["ticker"]
    schema: pd.DataFrame = pd.DataFrame(columns=[])

    def __init__(self, collectors):
        pass

weather_processor = WeatherProcessor(tiingoCollector, robinhoodCollector)