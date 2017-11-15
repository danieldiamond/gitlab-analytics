# Data model [WIP]

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

The general data flow would be SFDC->Talend->PG->Superset:
1. SFDC credentials are available to Talend via environment variables
1. A transformation file is in the repo, which describes which columns to retrieve and how to write them into PG
1. Talend is executed to ELT the data into PG
1. A Superset instance is spawned, connected to the PG DB
1. The dashboards for Superset are stored in the repo
