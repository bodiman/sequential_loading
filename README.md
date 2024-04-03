# Sequential Loading

This library provides a framework for loading data from multiple sources under a common interface with automated metadata collection. It is designed to be used in the context of collecting data from multiple sources, where the data is sparse and the sources are diverse. Using multiple sources can be advantageous when the data is sparse, unreliable, or when different sources provide complementary information.

The term "Sequential Loading" refers to the process of patching together a dataset by starting with a single data source and sequentially "filling in the gaps" using other sources. The Sequential Loading library provides a way to collect and store information about queries made to different data sources, and then synthesize a dataset from multiple sources.

# Motivation
When collecting sparsely-available data, it is often necessary to load data from multiple sources. Effectively patching together datasets requires an organized representation of 

1. What data is available from each source
2. The source of each collected datapoint

This library provides a framework for loading data from different sources under a common interface and a unified metadata representation. It provides a way to collect and store information about queries made to different data sources, and to store the data itself.

# Core Concepts

The objects in this library are organized around the following concepts: data, parameters, and metadata. These terms have specific meanings in the context of this library, which are discussed below.

## Data

Data refers to the actual information that is collected from a data source. Each datapoint is represented as a row in a typed pandas dataframe, whose schema is refered to as a `DataSchema`.

## Parameters

Parameters are inputs to a data collection query. They are represented as a row in a typed pandas dataframe, whose schema is refered to as a `ParameterSchema`. It is important to note that while parameters are inputs to a query, there may be additional query inputs that are not parameters.

A parameter should be an attribute of a datapoint that fundamentally distinguishes it from another datapoint. Each datapoint has a corresponding set of parameters, which is not necessarily unique. For instance, if one were to collect meteorological data, the parameters might include the date and location of the data. The time of day might also be an input to the query, but it would most likely not be a parameter.

Parameters define the "buckets" into which datapoints fall. Metadata is not tracked for individual datapoints, but for each unique set of parameters.

## Metadata

Metadata is information that is tracked about queries executed through a particular data processor. It is stored in a typed pandas dataframe, whose schema is refered to as a `MetaSchema`. The metaschema is defined uniquely for each processor. The metadata is tracked and updated for each query executed through a processor.

Metadata is tracked for each unique set of parameters. One example of metadata might be the time domain over which a data collector has been queried. In the case of a missing date in the data, this would allow one to determine weather the data is missing from the API, or if it has not been collected.


# Components
The Sequential Loading Library consists of 4 interfaces: Data Collectors, Data Processors, Data Storages, and Storage Datasets. Data Collectors are provide an interface for retriving data from a single source. Data Processors are responsible for tracking metadata about queries made to Data Processors. Data Storage is responsible for storing and retrieving metadata. Storage Datasets are responsible for synthesizing a dataset from data collected in the processors of a Data Storage.

## Data Collectors

The Data Collector class is a dependency inversion of an API. It consists of a retrieve_data method, which must match a specified schema, which is defined in a typed pandas dataframe.

As an example, one may write a Data Collector for a stock market api. First, one would define the schema for the data that is retrieved from the API.

```
#schemas.py

class StockSchema(TypedDataFrame):
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
```

The schema extends the TypedDataFrame class from the typeframe library. It must include a schema attribute, which is a dictionary that maps columns to data types.

Next, one would write a class to extend DataCollector, which implements the retrieve_data method. Instances of a class should have a name, which may either be unique to each instance, or shared among multiple instances.

```
from sequential_loading.data_collector import DataCollector
from schemas import StockSchema

class StockAPICollector(DataCollector):
    def __init__(self, name, api_key):
        super().__init__(name="TIINGO", schema=StockSchema)
        self.api_key = api_key

    def retrieve_data(self, stock_id, start_date, end_date):
        # Retrieve data from the API
        # This is pseudo code, not real syntax
        date, open, high, low, close, volume = http.get(f"https://api.tiingo.com/v1/stocks/{start_date}/{end_date}?token={self.api_key}")

        data = pd.DataFrame({
            "id": [uuid.uuid4() for _ in range(len(date))],
            "date": start_date,
            "open": open,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume
        })

        # Verify that data matches schema
        self.schema(data)

        return data
```

And that's all there is to it! The Data Collector is now ready to be used in a Data Processor.

## Data Processors

## Data Storages

## Storage Dataset

A storage Dataset is the interface for synthesizing a dataset from data collected in the processors of a Data Storage. Currently, only the Cached Storage is implemented, which does not take full advantage of multiple data sources. Future implementations will include:

1. Sequential dataset: A dataset created by sequentially loading data from multiple sources, useful for collecting sparse data
2. Summary dataset: A dataset created by taking the summary statistics of data loaded across different sources. Useful for collecting data where sources are not 100% reliable, and the summary statistics can be used to infer the true value.