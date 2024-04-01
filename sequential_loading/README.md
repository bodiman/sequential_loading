# Sequential Loading

# Motivation
When collecting sparsely-available data, it is often necessary to load data from multiple sources. Effectively patching together datasets requires an organized representation of 

1. what data is available from each source
2. the source of each collected datapoint

This library provides a framework for loading data from different sources under a common interface and a unified metadata representation. It provides a way to collect and store information about queries made to different data sources, and to store the data itself.

# Core Concepts

The objects in this library are organized around the following concepts: data, parameters, and metadata. These terms have specific meanings in the context of this library, which are discussed below.

# # Data

Data refers to the actual information that is collected from a data source. Each datapoint is represented as a row in a typed pandas dataframe, whose schema is refered to as a `DataSchema`.

# # Parameters

Parameters are inputs to a data collection query. They are represented as a row in a typed pandas dataframe, whose schema is refered to as a `ParameterSchema`. It is important to note that while parameters are inputs to a query, there may be additional query inputs that are not parameters. A parameter should be an attribute of a datapoint that fundamentally distinguishes it from another datapoint. For instance, if one were to collect meteorological data, the parameters might include the date and location of the data. The time of day might also be an input to the query, but it would most likely not be a parameter.

# # Metadata


# Data Collectors

# Data Processors

# Data Storages