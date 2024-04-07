import numpy as np
from typedframe import TypedDataFrame, DATE_TIME_DTYPE

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

    unique_constraint = ["id"]

class EODParamSchema(TypedDataFrame):
    schema = {
        "ticker": str, 
        "collector": str
    }


class WeatherParamSchema(TypedDataFrame):
    schema = {
        "location": str,
        "collector": str
    }

class WeatherSchema(TypedDataFrame):
    schema = {
        "id": str,
        "date": DATE_TIME_DTYPE,
        "tmin": np.int64,
        "tmax": np.int64,
        "tavg": np.float64,
        "cdd": np.float64,
        "precipitation": np.float64,
        "new_snow": np.float64
    }

    unique_constraint = ["id"]