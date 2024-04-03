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