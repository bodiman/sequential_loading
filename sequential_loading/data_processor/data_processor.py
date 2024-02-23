import pandas as pd

from typing import List

"""
Interface for synthesizing data and metadata from data collectors.

Members
-------
schema: pd.DataFrame
    Schema for data collection.

metaschema: pd.DataFrame
    Schema for metadata collection

data: pd.DataFrame
    Temporary storage for data. Reset to schema after storage.

metadata: pd.DataFrame
    Temporary storage for metadata. Reset to metaschema after storage.


Methods
-------



"""

class DataProcessor():
    def __init__(self, parameters: List[str], schema: pd.DataFrame) -> None:
        paramschema: pd.DataFrame = pd.DataFrame(columns=["id", "date", "collector", *parameters], dtypes=['str' for _ in range(len(parameters) + 3)])
        self.schema = pd.concat(paramschema, schema, verify_integrity=True)
        self.metaschema = pd.concat(paramschema, pd.DataFrame(columns=["domain"], dtypes=["str"]), verify_integrity=True)

        self.data: pd.DataFrame = self.schema
        self.metadata: pd.DataFrame = self.metaschema

