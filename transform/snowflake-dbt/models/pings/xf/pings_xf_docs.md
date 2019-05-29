{% docs pings_usage_data_monthly_change_by_stage %}

Monthly changes for usage statistics based on the cumulative monthly ping data, summarized into stages as per [ping_metrics_to_stage_mapping_data.csv](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/data/ping_metrics_to_stage_mapping_data.csv).

The following macros are used:
* stage_mapping - finds the specific columns to aggregate for each stage

{% enddocs %}