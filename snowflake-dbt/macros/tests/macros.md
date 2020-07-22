{% docs test_no_overlapping_valid_from_to_dates %}
See [dbt's documentation](https://docs.getdbt.com/docs/custom-schema-tests) on custom schema tests, which are implemented as macros.

This macro is a custom schema test to be used as a column test in a schema.yml file. It checks that there is a maximum of one valid row for that column on a selection of randomly selected dates. It expects that there are 2 other columns in the model: `valid_from` and `valid_to`. It was developed to be used on primary key columns in models built using the [SCD Type 2 macro](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/utils/scd_type_2.sql).


```
  - name: gitlab_dotcom_members
    columns:
      - name: member_id
        tests:
          - not_null
          - no_overlapping_valid_from_to_dates
```
{% enddocs %}


{% docs test_unique_where_currently_valid %}
See [dbt's documentation](https://docs.getdbt.com/docs/custom-schema-tests) on custom schema tests, which are implemented as macros.

This macro tests a column for uniqueness, but only checks rows with an `is_currently_valid` column with a value of True. This custom test was made specifically for models using the SCD macro and the default dbt uniquess test should be used in all other cases.

```
  - name: gitlab_dotcom_issue_links
    columns:
      - name: issue_link_id
        tests:
          - not_null
          - unique_where_currently_valid
```
{% enddocs %}
