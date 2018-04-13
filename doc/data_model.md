# Data Model [WIP]

In order to standardize and provide common functionality across companies, we have established a common data model based on best practices. As part of the transform phase, a mapping will need to be provided between your companies fields and the fields present in the common data model.  Once mapped, our analytics and dashboards can be leveraged without any further changes.

| Field | Data Type | Description |
| ----- | --------- | ----------- |
| Campaign Leads | Integer | Number of leads from campaign |
| Campaign Cost | Float | Cost of campaign |
| ... | String | More to come |

## Table types

* Staging: Raw imported data from each [data source](data_sources.md).
* Fact: Sales lines, gets really long
* Dimensions: A slice, for examples stages, can be slowly changing with time stamps or historical role
* Surrogate keys: Provides linkage between a fact to a dimension, an integer to store fewer bytes

## Data Flow

The general data flow would be [Source](data_sources.md)->[Extractor](data_sources.md#extractors)->PG Staging->dbt->PG Data Model:
1. Credentials for each source are available to the Extractor via CI environment variables
1. Schema: For Sprint 1, schema management and any necessarily translations will be done by hand. For Sprint 2, the extractor script will be responsible for the staging schema as well as any transformations, based on the meta data in the Source.
1. After the schema has been updated, the extractor loads the data into the a Source's staging table in PG
1. dbt will then be executed to pull in data from each staging table, normalize it with a hand created mapping, and load into the common [Data Model](#data model).
1. The data warehouse is then ready for use by a visualization engine like [Looker](https://looker.com).

## Key requirements
* We should strive to use Postgres only, and no SQL stored procedures as these live outside of version control.