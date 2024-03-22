from typedframe import TypedDataFrame
from typing import TypedDict
import pandas as pd

class LoaderSchema(TypedDataFrame):
    unique_constraint = None

    def __call__(self, df: pd.DataFrame):
        if df.empty:
            return df
        return super().__call__(df)
    
class CollectorResponse(TypedDict):
    data: pd.DataFrame
    status: str