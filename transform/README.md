## `generate_dbt_schema`

Creating dbt's `schema.yml` files is a requirement of the development process, but can be a bit tedious. This script creates one massive `schema.yml` file that creates a template for all of your dbt models, assuming they're in your `analytics` schema. You can just select the relevant lines for the models you're working on and paste them in your `schema.yml` file. 

It expects the following envrionment variables:
* SNOWFLAKE_ACCOUNT
* SNOWFLAKE_TRANSFORM_USER
* SNOWFLAKE_PASSWORD
* SNOWFLAKE_TRANSFORM_DATABASE
* SNOWFLAKE_SCHEMA
* SNOWFLAKE_TRANSFORM_WAREHOUSE
* SNOWFLAKE_TRANSFORM_ROLE
* SNOWFLAKE_TIMEZONE testing 123

Example:
```
SNOWFLAKE_ACCOUNT=gitlab
SNOWFLAKE_TRANSFORM_USER=user
SNOWFLAKE_PASSWORD=password
SNOWFLAKE_TRANSFORM_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=INFORMATION_SCHEMA 
SNOWFLAKE_TRANSFORM_WAREHOUSE=TRANSFORMING
SNOWFLAKE_TRANSFORM_ROLE=SYSADMIN
SNOWFLAKE_TIMEZONE=America/Los_Angeles
```

Note: `SNOWFLAKE_SCHEMA` is not used right now, but will be in the [next iteration](https://gitlab.com/meltano/analytics/issues/715).
