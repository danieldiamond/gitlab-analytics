## Action Type([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/gitlab_dotcom/action_type.sql))
This macro maps action type ID to the action type.
Usage:
```
{{action_type(1)}}
```
Used in:
- gitlab_dotcom_events.sql

## Alter Warehouse ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/warehouse/alter_warehouse.sql))
This macro turns on or off a Snowflake warehouse.
Usage:
```
{{resume_warehouse(var('resume_warehouse', false), var('warehouse_name'))}}
```
Used in:
- dbt_project.yml

## Case When Boolean Int ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/case_when_boolean_int.sql))
This macro returns a 1 if some value is greater than 0; otherwise, it returns a 0.
Usage:
```
{{ case_when_boolean_int("assignee_lists") }} AS assignee_lists_active
```
Used in:
- version_usage_data_boolean.sql

## Churn Type ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zuora/churn_type.sql))
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

## Create UDFs ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/udfs/create_udfs.sql))
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

## Distinct Sourse
This macro is used for condensing a `source` CTE into unique rows only. Our ETL runs quite frequently while most rows in our source tables don't update as frequently. So we end up with a lot of rows in our RAW tables that look the same as each other (except for the metadata columns with a leading underscore). This macro takes in a `source_cte` and looks for unique values across ALL columns. 

The 2 exception columns are:
* `_uploaded_at`: we only want the *minimum* value per unique row ... AKA "when did we *first* see this unique row?" This macros calls this column `valid_from` (to be used in the SCD Type 2 Macro)
* `_task_instance`: here we want to know the *maximum* task instance. This is used later to infer whether a `primary_key` is still present in the source table (as a roundabout way to track hard deletes.)

Usage:
```
WITH
{{ distinct_source(source=source('gitlab_dotcom', 'gitlab_subscriptions'))}}

, renamed AS ( ...
```


Used in:
- gitlab_dotcom_gitlab_subscriptions.sql
- gitlab_dotcom_issue_links.sql
- gitlab_dotcom_label_links.sql
- gitlab_dotcom_members.sql
- gitlab_dotcom_project_group_links.sql

## Generate Custom Schema ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/generate_custom_schema.sql))
This macro is used for implementing custom schemas for each model. For untagged models, the output is to the target schema (e.g. `emilie_scratch` and `analytics`). For tagged models, the output is dependent on the target. It is `emilie_scratch_staging` on dev and `analytics_staging` on prod. A similar pattern is followed for the `sensitive` config.
Usage:
```
{{ config(schema='staging') }}
```
Used in:
- all models surfaced in our BI tool.

## Get Internal Parent Namespaces
Returns a list of all the internal gitlab.com parent namespaces, enclosed in round brackets. This is useful for filtering an analysis down to external users only.

The internal namespaces are documented below.

| namespace | namespace ID |
| ------ | ------ |
| gitlab-com | 6543 |
| gitlab-org | 9970 |
| gitlab-data | 4347861 |
| charts | 1400979 |
| gl-recruiting | 2299361 |
| gl-frontend | 1353442 |
| gitlab-examples | 349181 |
| gl-secure | 3455548 |
| gl-retrospectives | 3068744 |
| gl-release | 5362395 |
| gl-docsteam-new | 4436569 |
| gl-legal-team | 3630110 |
| gl-locations | 3315282 |
| gl-serverless | 5811832 |
| gl-peoplepartners | 5496509 |
| gl-devops-tools | 4206656 |
| gl-compensation | 5495265 |
| gl-learning | 5496484 |
| meltano | 2524164 |

Usage:
```
{{ get_internal_parent_namespaces() }}
```
Used in:
- gitlab_dotcom/

## Grants ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/warehouse/grant_usage_to_schemas.sql))
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
IFF(issues.project_id IN ({{is_project_included_in_engineering_metrics()}}),
  TRUE, FALSE)                               AS is_included_in_engineering_metrics,
```

Used in:
- `gitlab_dotcom_issues_xf`
- `gitlab_dotcom_merge_requests_xf`


## Is Project Part of Product

This macro pulls all the engineering projects that are part of the product from
the seeded csv and adds a boolean in the model that can be used to filter on it.

Usage:
```
IFF(issues.project_id IN ({{is_project_part_of_product()}}),
  TRUE, FALSE)                               AS is_part_of_product,
```

Used in:
- `gitlab_dotcom_issues_xf`
- `gitlab_dotcom_merge_requests_xf`

## Monthly Change ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/monthly_change.sql))
This macro calculates differences for each consecutive usage ping by uuid.
Usage:
```
{{ monthly_change('active_user_count') }}
```
Used in:
- version_usage_data_monthly_change.sql

## Monthy Is Used ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/monthly_is_used.sql))
This macro includes the total counts for a given feature's usage cumulatively.
Usage:
```
{{ monthly_is_used('auto_devops_disabled') }}
```
Used in:
- version_usage_data_monthly_change.sql

## Product Category([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zuora/product_category.sql))
This macro maps SKUs to their product categories.
Usage:
```
{{product__category('rate_plan_name')}}
```
Used in:
- sfdc_opportunity.sql
- zuora_rate_plan.sql

## Delivery([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zuora/delivery.sql))
This macro maps product categories to [delivery](https://about.gitlab.com/handbook/marketing/product-marketing/tiers/#delivery).

Usage:
```
{{ delivery('product_category') }}
```
Used in:
- zuora_rate_plan.sql

## Resource Label Action Type([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/gitlab_dotcom/resource_label_action_type.sql))
This macro maps action type ID to the action type for the `resource_label_events` table.
Usage:
```
{{ resource_label_action_type('action') }}
```

Used in:
- gitlab_dotcom_resource_label_events.sql

## Sales Segment Cleaning([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/sfdc/sales_segment_cleaning))
This macro applies proper formatting to sales segment data with the end result being one of SMB, Mid-Market, Strategic, Large or Unknown.
Usage:
```
{{sales_segment_cleaning("column_1")}}
```
Used in:
- sfdc_opportunity.sql
- zendesk_organizations.sql
- sfdc_lead.sql

## Schema Union All ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/schema_union_all.sql))
This macro takes a schema prefix and a table name and does a UNION ALL on all tables that match the pattern.
Usage:
```
{{ schema_union_all('snowplow', 'snowplow_page_views') }}
```
Used in:
- snowplow_combined/all/*.sql
- snowplow_combined/30/*.sql

## Schema Union Limit ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/schema_union_limit.sql))
This macro takes a schema prefix, a table name, a column name, and an integer representing days. It returns a view that is limited to the last 30 days based on the column name. Note that this also calls schema union all which can be a heavy call.
Usage:
```
{{ schema_union_limit('snowplow', 'snowplow_page_views', 'page_view_start', 30) }}
```
Used in:
- snowplow_combined/30_day/*.sql

## SCD Type 2
This macro inserts SQL statements that turn the inputted CTE into a [type 2 slowly changing dimension model](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row).

From [Orcale](https://www.oracle.com/webfolder/technetwork/tutorials/obe/db/10g/r2/owb/owb10gr2_gs/owb/lesson3/slowlychangingdimensions.htm): "A Type 2 SCD retains the full history of values. When the value of a chosen attribute changes, the current record is closed. A new record is created with the changed data values and this new record becomes the current record. Each record contains the effective time and expiration time to identify the time period between which the record was active."

In particular, this macro adds 3 columns: `valid_from`, `valid_to`, and `is_currently_valid`.
`valid_from` will never be null, while `valid_to` can be NULL for one row per ID (`is_currently_active` will be TRUE in that case). It is possible for an ID to have 0 currently active rows (implies a "Hard Delete" on the source db.)

This macro was built to be used in conjunction with the `distinct_source` macro. 

The parameters are as follows:
  * **primary_key_renamed**: The primary key column from the `casted_cte` 
  * **primary_key_raw**: The same column as above, but use the name from when it was in the RAW schema. Value is usually `id`.
  * **source_cte**: (defaults to '`distinct_source`). This is the name of the CTE with all of the unique source rows. This will always be `distinct_source` if using the `distinct_source` macro.
  * **casted_cte**: (defaults to `renamed`) . This is the name of the CTE with all of the casted/renamed column names. Our internal convention is to call this `renamed`.

Usage:
```
, renamed AS (
  ... 
)

{{ scd_type_2(
    primary_key_renamed='project_group_link_id',
    primary_key_raw='id'
) }}
```


Used in:
- gitlab_dotcom_gitlab_subscriptions.sql
- gitlab_dotcom_issue_links.sql
- gitlab_dotcom_label_links.sql
- gitlab_dotcom_members.sql
- gitlab_dotcom_project_group_links.sql

## SFDC Deal Size ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/sfdc/sfdc_deal_size.sql))
This macro buckets a unit into a deal size (Small, Medium, Big, or Jumbo) based on an inputted value.
Usage:
```
{{sfdc_deal_size('incremental_acv', 'deal_size')}}
```
Used in:
- sfdc_opportunity.sql
- sfdc_account_deal_size_segmentation.sql

## SFDC Source Buckets ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/sfdc/sfdc_source_buckets.sql))
This macro is a CASE WHEN statement that groups the lead sources into new marketing-defined buckets. @rkohnke is the DRI on any changes made to this macro.
Usage:
```
{{  sfdc_source_buckets('leadsource') }}
```
Used in:
- sfdc_contact
- sfdc_lead
- sfdc_opportunity

## SMAU Events CTES

This macro is designed to build the pageview events CTEs that are then used in all the `snowplow_smau_events` models. Please [read this documentation](https://about.gitlab.com/direction/telemetry/smau_events/#events-summary-table) for more context about SMAU pageview events. This expects a CTE to exist called `snowplow_pageviews`. This CTE generally look like this:

```
WITH snowplow_page_views AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    page_view_start,
    page_url_path,
    page_view_id,
    referer_url_path
  FROM {{ ref('snowplow_page_views_all') }}
  WHERE TRUE
    AND app_id = 'gitlab'
  {% if is_incremental() %}
    AND page_view_start >= (SELECT MAX(event_date) FROM {{this}})
  {% endif %}

)
```

It takes 2 parameters:
* `event_name`: which is the name shown in output tables and Periscope reporting
* `regexp_where_statements`: which is a list of dictionaries. Each dictionary creates a new condition in the WHERE statement of the CTE. The dictionary will have 2 items:
  * `regexp_pattern`: the pattern that you try to match
  * `regexp_function`: the function used (either `REGEXP` or `NOT REGEXP`)
  The conditions created by a dictionary looks like: `page_url_path {regexp_function} '{regexp_pattern}'`. Conditions are always separated by an `AND`.

Usage:
```
{{  smau_events_ctes(action_name="pipeline_schedules_viewed",
                     regexp_where_statements=[
                                               {
                                                  "regexp_pattern":"(\/([0-9A-Za-z_.-])*){2,}\/pipeline_schedules",
                                                  "regexp_function":"REGEXP"
                                               }]
                                               )
}}
```

Output:
```
pipeline_schedules_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start)    AS event_date,
    page_url_path,
    'pipeline_schedules_viewed' AS event_type,
    page_view_id                AS event_surrogate_key

  FROM snowplow_page_views
  WHERE TRUE
    AND page_url_path REGEXP '(\/([0-9A-Za-z_.-])*){2,}\/pipeline_schedules'


)
```

Used in:
- configure_snowplow_smau_events
- create_snowplow_smau_events
- manage_snowplow_smau_events
- monitor_snowplow_smau_events
- package_snowplow_smau_events
- plan_snowplow_smau_events
- release_snowplow_smau_events
- verify_snowplow_smau_events

## Stage Mapping ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/version/stage_mapping.sql))
This macro takes in a product stage name, such as 'Verify', and returns a SQL aggregation statement that sums the number of users using that stage, based on the ping data. Product metrics are mapped to stages using the [ping_metrics_to_stage_mapping_data.csv](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/data/ping_metrics_to_stage_mapping_data.csv).
```
{{ stage_mapping( 'Verify' ) }}
```
Used in:
- version_usage_data_monthly_change_by_stage.sql

## Support SLA Met ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zendesk/support_sla_met.sql))
This macro implements the `CASE WHEN` logic for Support SLAs, as [documented in the handbook](https://about.gitlab.com/support/#gitlab-support-service-levels).
```
{{ support_sla_met( 'first_reply_time',
                    'ticket_priority',
                    'ticket_created_at') }} AS was_support_sla_met

```
Used in:
- zendesk_tickets_xf.sql

## Unpack Unstructured Events ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/version/unpack_unstructured_event.sql))
This macro unpacks the unstructured snowplow events. It takes a list of field names, the pattern to match for the name of the event, and the prefix the new fields should use.
Usage:
```
{{ unpack_unstructured_event(change_form, 'change_form', 'cf') }}
```
Used in:
- snowplow_fishtown_unnested_events.sql
- snowplow_gitlab_events.sql

## User Role Mapping
This macro maps "role" values (integers) from the user table into their respective string values.

For example, user_role=0 maps to the 'Software Developer' role.

Used in:
- gitlab_dotcom_users.sql

## Zuora Slugify ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zuora/zuora_slugify.sql))
This macro replaces any combination of whitespace and 2 pipes with a single pipe (important for renewal subscriptions) and it replaces all non alphanumeric characters with dashes and casts it to lowercases as well. The end result of using this macro on data like "A-S00003830 || A-S00013333" is "a-s00003830|a-s00013333".
Usage:
```
{{zuora_slugify("name")}}
```
Used in:
- zuora_subscription.sql
- customers_db_orders.sql
