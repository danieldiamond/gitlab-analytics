# So a test has failed!

This document exists to capture common fixes for problems that may be surfaced by our dbt test suite.

You will find the below document organized by test name, containing all documented errors for that test.

**Most tests are meant to capture data quality concerns, so it's more important to investigate the cause of the failure than it is to remove the failure.**

## Types of test

There are two types of tests, `custom data tests` and `schema tests`.

You can tell which kind of test this error was surfaced from by looking at the path at the end of the error message, if `tests/data_test/*`
this is a `custom data test` which is defined as a .sql file in the `tests` folder, the compiled sql will be found in your `target/compiled/gitlab_snowflake/data_test` or `target/compiled/snowplow/data_test` folder.

If it's `models/*` this is a `schema test` and is defined in the `schema.yml` file on the model's folder and the compiled sql in the `target/compiled/gitlab_snowflake/schema_test` or `target/compiled/snowplow/schema_test` folder.


## General Checklist:
1. Make sure you are running the compiled code behind the test against `analytics` instead of your development schema
2. Verify that the error is valid by reviewing the data that is surfacing the error in the following order of priorities: test-failure --> model that was built powering test --> base model --> raw data
3. Go to the front-end of the data source and validate that it is not a data pipeline error
4. Create an issue in the Primary Data Project. If the issue needs to contain customer names or other private information ensure the issue is confidential

## Custom Data Tests

### Test: zuora_renewal_subscription_date_range

-----------
Error Example:
```
Failure in test zuora_renewal_subscription_date_range (tests/data_test/zuora_renewal_subscription_date_range.sql)
  Got 1 results, expected 0.

  compiled SQL at target/compiled/gitlab_snowflake/data_test/zuora_renewal_subscription_date_range.sq
```

Steps to Resolve:

* Step 1: Follow the general checklist
* Step 2: Create an issue in finance informing of the error and asking the account be updated. Cross link this to the analytics issue
* Step 3: Create an issue to remove the filter and assign it to the next milestone, cross-link it to the original issue
* Step 4: Filter out the zuora account in the base `zuora_account` model and submit your MR for review
  * We filter from the base model instead of the test because downstream models (such as retention) rely on every account having accurate data.
* Step 5: Once finance has confirmed that the account has been updated, create a MR to remove the filter
-----------

### Test: zuora_account_has_crm_id

Error Example:
```
Failure in test zuora_account_has_crm_id (tests/data_test/zuora_account_has_crm_id.sql)
  Got 1 results, expected 0.

  compiled SQL at target/compiled/gitlab_snowflake/data_test/zuora_account_has_crm_id.sql

```

Steps to Resolve:

* Step 1: Follow the general checklist
* Step 2: Create an issue in finance asking the account get updated with a salesforce_id. Cross link this to the analytics issue
* Step 3: Create an issue to remove the filter and assign it to the next milestone, cross-link it to the original issue
* Step 4: Filter out the zuora account in the base `zuora_account` model and submit your MR for review
  * We filter from the base model instead of the test because downstream models (such as retention) rely on every account having accurate data.
* Step 5: Once finance has confirmed that the account has been updated, create a MR to remove the filter


### Test: zuora_assert_single_ult_parent
Error Example:
```
Failure in test zuora_assert_single_ult_parent (tests/data_test/zuora_assert_single_ult_parent.sql)
  Got 1 results, expected 0.

  compiled SQL at target/compiled/gitlab_snowflake/data_test/zuora_assert_single_ult_parent.sql
```

Steps to Resolve:

* Step 1: Follow the general checklist
* Step 2: Create an issue in finance asking for the Zuora account and subscription linkages to be reviewed.
    * If the data on the Zuora end is fine, then bring in Sales people to review Salesforce data.
* Step 3: Create an issue to remove the filter and assign it to the next milestone, cross-link it to the original issue
* Step 4: Filter out the zuora subscription in the test based on the md5 has of the `ultimate_parent_sub` name
* Step 5: Once finance has confirmed that the accounts and subscriptions have been updated, create a MR to remove the filter

### Test: zuora_assert_no_circular_linkages

This custom test asserts `zuora_subscription_intermediate` has no circular linkage. A circular linkage is created when the `zuora_renewal_subscription_name_slugify` is equal to the `subscription_name_slugify` in the `zuora_subscription_intermediate`.

When the test fails, the `zuora_subscription_lineage` model will fail with the following error:
```
Database Error in model zuora_subscription_lineage (models/zuora/xf/zuora_subscription_lineage.sql)
  100189 (22000): 018d9a6c-01bb-a7c9-0000-289d0753e48e: Recursion exceeded max iteration count (100).
```


Steps to Resolve:

* Step 1: Follow the general checklist
* Step 2: Create an issue in finance asking for the Zuora subscription lineage to be reviewed.
* Step 3: Create an issue to remove the filter and assign it to the next milestone, cross-link it to the original issue
* Step 4: Filter out the zuora account in the base `zuora_subscription` model and submit your MR for review
  * We filter from the base model instead of the test because downstream models (such as retention) will fail otherwise
* Step 5: Once finance has confirmed that the account has been updated, create a MR to remove the filter

### Test: uncategorized_pings
This test checks that the list of unique ping metrics that we receive, `pings_list`, matches the ping metrics that we have categorized in the static CSV, `ping_metrics_to_stage_mapping_data`. This test will fail when these two sources get out of sync in either direction.

Error Example:
```
Database Error in model pings_usage_data_monthly_change_by_stage (models/pings/xf/pings_usage_data_monthly_change_by_stage.sql)
  000904 (42000): 018c7f13-0141-8466-0000-289d05dda8ae: SQL compilation error: error line 397 at position 24
  invalid identifier 'PINGS.OPERATIONS_DASHBOARD_DEFAULT_DASHBOARD_CHANGE'
  compiled SQL at target/compiled/gitlab_snowflake/pings/xf/pings_usage_data_monthly_change_by_stage.sql
```

Steps to Resolve:

* Step 1: Follow the general checklist
* Step 2: Check that the rows of [ping_metrics_to_stage.csv](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/data/ping_metrics_to_stage_mapping_data.csv) match the ping names in `analytics_staging.pings_list`. If `pings_list` has more rows than the CSV, the pings data has likely changed and the CSV may need to be updated to reflect that.

## Schema Tests

### Model: snowplow

Most snowplow errors can be resolved by triggering a full dbt refresh as the models are incremental, but sometimes have late-arriving events. Request a full refresh to @tmurphy in the #analytics-pipelines Slack channel.

Snowplow contains models and tests that are baked in from a Package (docs [can be found here](https://github.com/fishtown-analytics/snowplow)), so make sure
to check the compiled test (path found in the `compiled SQL` section of the error message) for test evaluation.

Error Example:

```
Failure in test relationships_snowplow_web_events_time_page_view_id__page_view_id__ref_snowplow_web_page_context_ (models/page_views/schema.yml)
  Got 15164 results, expected 0.

  compiled SQL at target/compiled/snowplow/schema_test/relationships_snowplow_web_events_time_4183e7f72f68f16d48e4f78cd66a9c48.sql
```

### Models : `sfdc_users_archived` and `sfdc_account_archived`

The failing tests are the following:

```
tests:
  - dbt_utils.recency:
      datepart: day
      field: dbt_last_updated_timestamp
      interval: 1
```

These schema tests assert that `dbt archive` has run successfully and the last inserted data in `sfdc_users_archived` and `sfdc_account_archived` tables are less than 24 hours old.

These tests are failing when `dbt test` is run before `dbt archive`. So dbt tests freshness of the data of the archived tables before the archivals are actually finished.

Nothing is expected from triager when this happens for the moment. [An issue](https://gitlab.com/gitlab-data/analytics/issues/1725) has been opened to make snapshots more robust.

Error Example:

```
Failure in test recency_sfdc_account_archived_day__dbt_last_updated_timestamp__1 (models/sfdc/base/schema.yml)\x1b[0m\n'

Got 1 results, expected 0.

compiled SQL at target/compiled/dbt_utils/schema_test/recency_sfdc_account_archived_f4f936fbfa6592180d13b0c5befeafac.sql

```
