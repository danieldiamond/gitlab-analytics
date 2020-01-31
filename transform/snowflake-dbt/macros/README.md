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

## Backup to GCS ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/warehouse/backup_to_gcs.sql))
This macro fetches all relevant tables in the specified database and schema for backing up into GCS. This macro should NOT be used outside of a `dbt run-operation` command.

Used in:
- dags/general/dbt_backups.py

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
- retention_reasons_for_retention.sql
- retention_parent_account_.sql
- retention_sfdc_account_.sql
- retention_zuora_subscription_.sql

## Retention Type ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zuora/churn_type.sql))
This macro compares MRR values and buckets them into retention categories.
Usage:
```
{{ retention_type(original_mrr, new_mrr) }}
```
Used in:
- retention_reasons_for_retention.sql

## Retention Reason ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zuora/retention_reason.sql))
This macro compares MRR values, Product Category, Product Ranking and Amount of seats and gives back the reason as to why MRR went up or down.
Usage:
```
{{ retention_reason(original_mrr, original_product_category, original_product_ranking,
                           original_seat_quantity, new_mrr, new_product_category, new_product_ranking,
                           new_seat_quantity) }}
```
Used in:
- retention_reasons_for_retention.sql

## Monthly Price Per Seat Change ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zuora/price_per_seat_change.sql))
This macro calculates the difference between monthly price per seat, but only when the unit_of_measure of the plan is seats.
Usage:
```
{{ price_per_seat_change(original_mrr, original_seat_quantity, original_unit_of_measure,
                                new_mrr, new_seat_quantity, new_unit_of_measure) }}
```
Used in:
- retention_reasons_for_retention.sql

## Seat Change ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zuora/seat_change.sql))
This macro compares the amount of seats and categorizes it into Maintained/Contraction/Expansion/Cancelled, and when the unit_of_measure of the plans isn't seats, Not Valid
Usage:
```
{{ seat_change(original_seat_quantity, original_unit_of_measure, original_mrr, new_seat_quantity, new_unit_of_measure, new_mrr) }}
```
Used in:
- retention_reasons_for_retention.sql

## Plan Change ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zuora/plan_change.sql))
This macro compares product rankings and returns whether it was upgraded/downgraded/maintained/cancelled, and when the product ranking is 0 (which means it's an old plan) it returns Not Valid
Usage:
```
{{ plan_change(original_product_ranking, original_mrr, new_product_ranking, new_mrr) }}
```
Used in:
- retention_reasons_For_retention.sql

## Coalesce to Infinity
This macro expects a timestamp or date column as an input. If a non-null value is inputted, the same value is returned. If a null value is inputted, a large date representing 'infinity' is returned. This is useful for writing `BETWEEN` clauses using date columns that are sometimes NULL.

Used in:
- gitlab_dotcom_issues_xf.sql
- gitlab_dotcom_merge_requests_xf.sql
- gitlab_dotcom_projects_xf.sql
- gitlab_dotcom_usage_data_events.sql
- version_usage_data_weekly_opt_in_summary.sql

## Create Snapshot Base Models ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/create_snapshot_base.sql))
This macro creates a base model for dbt snapshots. A single entry is generated from the chosen start date through the current date for the specified primary key(s) and unit of time. 
Usage:
```
"{{create_snapshot_base(source, primary_key, date_start, date_part, snapshot_id_name)}}"
```
Used in:
- sfdc_opportunity_snapshots_base.sql

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

## Distinct Source ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/distinct_source.sql))
This macro is used for condensing a `source` CTE into unique rows only. Our ETL runs quite frequently while most rows in our source tables don't update as frequently. So we end up with a lot of rows in our RAW tables that look the same as each other (except for the metadata columns with a leading underscore). This macro takes in a `source_cte` and looks for unique values across ALL columns (excluding airflow metadata.)  

This macro **is specific** to pgp tables (gitlab_dotcom, version, license) and should not be used outside of those. Specifically, it makes references to 2 airflow metadata columns:
* `_uploaded_at`: we only want the *minimum* value per unique row ... AKA "when did we *first* see this unique row?" This macros calls this column `valid_from` (to be used in the SCD Type 2 Macro)
* `_task_instance`: we want to know the *maximum* task instance (what was the last task when we saw this row?). This is used later to infer whether a `primary_key` is still present in the source table (as a roundabout way to track hard deletes)

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

## Get Backup Table Command ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/warehouse/get_backup_table_command.sql))
This macro is called by `backup_to_gcs` so that the actual `copy into` command can be generated. This macro should NOT be referenced outside of the `backup_to_gcs` macro.
```
{% set backup_table_command = get_backup_table_command(table, day_of_month) %}
{{ backup_table_command }}
```
Used in:
- backup_to_gcs.sql

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

## Max Date in Bamboo HR ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/max_date_in_bamboo_analyses.sql))
This macro creates a reusable variable based on the current date. 
Usage:
```
{{ max_date_in_bamboo_analyses() }}
```
Used in: 
- `rpt_team_members_out_of_comp_band`
- `bamboohr_employment_status_xf`
- `employee_directory_intermediate`

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
- sfdc_opportunity_field_historical.sql
- sfdc_opportunity_snapshot_history.sql
- zendesk_organizations.sql
- sfdc_lead.sql

## Schema Union All ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/schema_union_all.sql))
This macro takes a schema prefix and a table name and does a UNION ALL on all tables that match the pattern. The exclude_part parameter defaults to 'scratch' and all schemas matching that pattern will be ignored. 
Usage:
```
{{ schema_union_all('snowplow', 'snowplow_page_views') }}
```
Used in:
- snowplow_combined/30_day/*.sql
- snowplow_combined/90_day/*.sql
- snowplow_combined/all/*.sql

## Schema Union Limit ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/schema_union_limit.sql))
This macro takes a schema prefix, a table name, a column name, and an integer representing days. It returns a view that is limited to the last 30 days based on the column name. Note that this also calls schema union all which can be a heavy call.
Usage:
```
{{ schema_union_limit('snowplow', 'snowplow_page_views', 'page_view_start', 30) }}
```
Used in:
- snowplow_combined/30_day/*.sql
- snowplow_combined/90_day/*.sql

## SCD Type 2 ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/scd_type_2.sql))
This macro inserts SQL statements that turn the inputted CTE into a [type 2 slowly changing dimension model](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row). According to [Orcale](https://www.oracle.com/webfolder/technetwork/tutorials/obe/db/10g/r2/owb/owb10gr2_gs/owb/lesson3/slowlychangingdimensions.htm), "a Type 2 SCD retains the full history of values. When the value of a chosen attribute changes, the current record is closed. A new record is created with the changed data values and this new record becomes the current record. Each record contains the effective time and expiration time to identify the time period between which the record was active."

In particular, this macro adds 3 columns: `valid_from`, `valid_to`, and `is_currently_valid`. It does not alter or drop any of the existing columns in the input CTE.
* `valid_from` will never be null
* `valid_to` can be NULL for up to one row per ID. It is possible for an ID to have 0 currently active rows (implies a "Hard Delete" on the source db)
* `is_currently_active` will be TRUE in cases where `valid_to` is NULL (for either 0 or 1 rows per ID)

The parameters are as follows:
  * **primary_key_renamed**: The primary key column from the `casted_cte`. According to our style guide, we usually rename primary keys to include the table name ("merge_request_id")
  * **primary_key_raw**: The same column as above, but references the column name from when it was in the RAW schema (usually "id")
  * **source_cte**: (defaults to '`distinct_source`). This is the name of the CTE with all of the unique rows from the raw source table. This will always be `distinct_source` if using the `distinct_source` macro.
  * **casted_cte**: (defaults to `renamed`). This is the name of the CTE where all of the columns have been casted and renamed. Our internal convention is to call this `renamed`. This CTE needs to have a column called `valid_from`.

This macro does **not** reference anything specific to the pgp data sources, but was built with them in mind. It is unlikely that this macro will be useful to anything outside of pgp data sources as it was built for a fairly specific problem. We would have just used dbt snapshots here except for the fact that they currently don't support hard deletes. dbt snapshots should be satisfactory for most other use cases.

This macro was built to be used in conjunction with the [distinct_source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/distinct_source.sql)) macro.

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
- sfdc_opportunity_field_historical.sql
- sfdc_opportunity_snapshot_history.sql
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
- sfdc_opportunity_field_historical.sql
- sfdc_opportunity_snapshot_history.sql

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

## Test No Overlapping Valid From To Dates ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/tests/test_no_overlapping_valid_from_to_dates.sql))
This macro is a custom schema test to be used as a column test in a schema.yml file. It checks that there is a maximum of one valid row for that column on a selection of randomly selected dates. It expects that there are 2 other columns in the model: `valid_from` and `valid_to`. It was developed to be used on primary key columns in models built using the [SCD Type 2 macro](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/scd_type_2.sql).

```
  - name: gitlab_dotcom_members
    columns:
      - name: member_id
        tests:
          - not_null
          - no_overlapping_valid_from_to_dates
```

Used in:
- gitlab_dotcom/base/schema.yml

## Test Unique Where Currently Valid([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/tests/test_unique_where_currently_valid.sql))
This macro tests a column for uniqueness, but only checks rows with an `is_currently_valid` column with a value of True. This custom test was made specifically for models using the SCD macro and the default dbt uniquess test should be used in all other cases. 

```
  - name: gitlab_dotcom_issue_links
    columns:
      - name: issue_link_id
        tests:
          - not_null
          - unique_where_currently_valid
```

Used in:
- gitlab_dotcom/base/schema.yml

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
This macro replaces any combination of whitespace and 2 pipes with a single pipe (important for renewal subscriptions), replaces any multiple whitespace characters with a single whitespace character, and then it replaces all non alphanumeric characters with dashes and casts it to lowercases as well. The end result of using this macro on data like "A-S00003830 || A-S00013333" is "a-s00003830|a-s00013333".

The custom test `zuora_slugify_cardinality` tests the uniqueness of the `zuora_subscription_slugify` (eg. 2 different subscription names will result to 2 different `zuora_subscription_name_slugify`)

Usage:
```
{{zuora_slugify("name")}}
```
Used in:
- zuora_subscription.sql
- customers_db_orders.sql

## Zuora Excluded Accounts ([Source](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/zuora/zuora_excluded_accounts.sql))
This macro returns a list of zuora account_ids that are meant to be excluded from our base models. Account IDs can be filtered out because they were created for internal testing purposes (permanent filter) or because there's a data quality issue ([like a missing CRM](https://gitlab.com/gitlab-data/analytics/tree/master/transform/snowflake-dbt/tests#test-zuora_account_has_crm_id)) that we're fixing (temporary filter).

- Used in:
- zuora_account.sql
- zuora_contact
- zuora_invoice
- zuora_refund
- zuora_subscription.sql