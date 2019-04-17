## Alter Warehouse ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/alter_warehouse.sql))
This macro turns on or off a Snowflake warehouse. 
Usage:
```
{{resume_warehouse(var('resume_warehouse', false), var('warehouse_name'))}}
```
Used in:
- dbt_project.yml

## Case When Boolean Int ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/case_when_boolean_int.sql))
This macro returns a 1 if some value is greater than 0; otherwise, it returns a 0.
Usage:
```
{{ case_when_boolean_int("assignee_lists") }} AS assignee_lists_active
```
Used in: 
- pings_usage_data_boolean.sql

## Churn Type ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/churn_type.sql))
Usage:
```
{{churn_type()}}
```
Used in:
- retention_parent_account_.sql
- retention_sfdc_account_.sql
- retention_zuora_subscription_.sql

## Create UDFs ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/create_udfs.sql))
This macro is inspired by [this discourse post](https://discourse.getdbt.com/t/using-dbt-to-manage-user-defined-functions-redshift/18) on using dbt to manager UDFs.
Usage:
```
"{{create_udfs()}}"
```
Used in:
- dbt_project.yml

## Generate Custom Schema ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/generate_custom_schema.sql))
This macro is used for implementing custom schemas for each model. For untagged models, the output is to the target schema suffixed with `_staging` (e.g. `emilie_scratch_staging` and `analytics_staging`). For tagged models, the output is dependent on the target. It is `emilie_scratch_analytics` on dev and `analytics` on prod. 
Usage: 
```
{{ config(schema='analytics') }}
```
Used in: 
- all models surfaced in our BI tool.

## Get Pings JSON Keys ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/get_pings_json_keys.sql))
This macro gets the path and field name for each stat pushed to pings `stats_used`

Usage:
```
{{get_pings_json_keys()}}
```

After running this macro, the data is available using

```load_result('stats_used')['data'] ```

which will results an array of tuples with value 0 being the JSON path and value 1 being the field name to use. 

The end result will look something like 

```[('operation_dashboard.default', 'operation_dashboard_default), ('ci_pipeline_config_auto_devops', 'ci_pipeline_config_auto_devops')...]```

Used in:
- pings_usage_data_unpacked.sql
- pings_usage_data_monthly_change.sql
- pings_data_month.sql
- pings_usage_data_boolean.sql

## Grants ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/grant_usage_to_schema.sql))
This macro...
Usage:
```
on-run-end:
 - "{{ grant_usage_to_schemas(schemas, user) }}"
```
Used in:
- dbt_project.yml

## Monthly Change ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/monthly_change.sql))
This macro calculates differences for each consecutive usage ping by uuid.
Usage:
```
{{ monthly_change('active_user_count') }}
```
Used in:
- pings_usage_data_monthly_change.sql

## Monthy Is Used ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/monthly_is_used.sql))
This macro includes the total counts for a given feature's usage cumulatively.
Usage:
```
{{ monthly_is_used('auto_devops_disabled') }}
```
Used in:
- pings_usage_data_monthly_change.sql

## SFDC Deal Size ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/sfdc_deal_size.sql))
This macro buckets a unit into a deal size (Small, Medium, Big, or Jumbo) based on an inputted value.
Usage:
```
{{sfdc_deal_size('incremental_acv', 'deal_size')}}
```
Used in:
- sfdc_opportunity.sql
- sfdc_account_deal_size_segmentation.sql

## Zuora Slugify ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zuora_slugify.sql))
This macro replaces replaces any combination of whitespace and 2 pipes with a single pipe (important for renewal subscriptions) and it replaces all non alphanumeric characters with dashes and casts it to lowercases as well. The end result of using this macro on data like "A-S00003830 || A-S00013333" is "a-s00003830|a-s00013333".
Usage:
```
{{zuora_slugify("name")}}
```
Used in:
- zuora_subscription.sql


## Sales Segment Cleaning([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/sales_segment_cleaning))
This macro applies proper formatting to sales segment data with the end result being one of SMB, Mid-Market, Strategic, Large or Unknown.
Usage:
```
{{sales_segment_cleaning("column_1")}}
```
Used in:
- sfdc_opportunity.sql 
- zendesk_organizations.sql
