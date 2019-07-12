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
This macro compares MRR values and buckets them into retention categories.
Usage:
```
{{ churn_type(original_mrr, new_mrr) }}
```
Used in:
- retention_reasons_For_retention.sql
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

## dbt Logging
This macro logs some output to the command line. It can be used in a lot of ways.
Usage:
```
"{{ dbt_logging_start('on run start hooks') }}"
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

## Get Internal Namespaces
Returns a list of all the internal gitlab.com namespaces, enclosed in round brackets. This is useful for filtering an analysis down to external users only.
Usage:
```
{{ get_internal_namespaces() }}
```
Used in:
- gitlab_dotcom/

## Grants ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/grant_usage_to_schema.sql))
This macro...
Usage:
```
on-run-end:
 - "{{ grant_usage_to_schemas(schemas, user) }}"
```
Used in:
- dbt_project.yml

## Is Project Included In Engineering Metrics
This macro pulls all the engineering projects to be included from the seeded csv and adds a boolean in the model that can be used to filter on it.
Usage:
```
CASE WHEN issues.project_id IN ({{is_project_included_in_engineering_metrics()}}) THEN TRUE
     ELSE FALSE END AS is_included_in_engineering_metrics,
```
Used in:
- gitlab_dotcom_issues_xf
- gitlab_dotcom_merge_requests_xf

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

## Product Category([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/product_category.sql))
This macro maps SKUs to their product categories.
Usage:
```
{{product__category('rate_plan_name')}}
```
Used in:
- sfdc_opportunity.sql
- zuora_rate_plan.sql

## Sales Segment Cleaning([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/sales_segment_cleaning))
This macro applies proper formatting to sales segment data with the end result being one of SMB, Mid-Market, Strategic, Large or Unknown.
Usage:
```
{{sales_segment_cleaning("column_1")}}
```
Used in:
- sfdc_opportunity.sql
- zendesk_organizations.sql
- sfdc_lead.sql

## SFDC Deal Size ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/sfdc_deal_size.sql))
This macro buckets a unit into a deal size (Small, Medium, Big, or Jumbo) based on an inputted value.
Usage:
```
{{sfdc_deal_size('incremental_acv', 'deal_size')}}
```
Used in:
- sfdc_opportunity.sql
- sfdc_account_deal_size_segmentation.sql

## Stage Mapping ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/stage_mapping.sql))
This macro takes in a product stage name, such as 'Verify', and returns a SQL aggregation statement that sums the number of users using that stage, based on the ping data. Product metrics are mapped to stages using the [ping_metrics_to_stage_mapping_data.csv](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/data/ping_metrics_to_stage_mapping_data.csv).
```
{{ stage_mapping( 'Verify' ) }}
```
Used in:
- pings_usage_data_monthly_change_by_stage.sql

## Unpack Unstructured Events ([Source]())
This macro unpacks the unstructured snowplow events. It takes a list of field names, the pattern to match for the name of the event, and the prefix the new fields should use.
Usage:
```
{{ unpack_unstructured_event(change_form, 'change_form', 'cf') }}
```
Used in:
- snowplow_fishtown_unnested_events.sql
- snowplow_gitlab_events.sql

## Zuora Slugify ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zuora_slugify.sql))
This macro replaces any combination of whitespace and 2 pipes with a single pipe (important for renewal subscriptions) and it replaces all non alphanumeric characters with dashes and casts it to lowercases as well. The end result of using this macro on data like "A-S00003830 || A-S00013333" is "a-s00003830|a-s00013333".
Usage:
```
{{zuora_slugify("name")}}
```
Used in:
- zuora_subscription.sql
- customers_db_orders.sql
