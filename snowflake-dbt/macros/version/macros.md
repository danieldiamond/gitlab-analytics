{% docs stage_mapping %}
This macro takes in a product stage name, such as 'Verify', and returns a SQL aggregation statement that sums the number of users using that stage, based on the ping data. Product metrics are mapped to stages using the [ping_metrics_to_stage_mapping_data.csv](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/data/ping_metrics_to_stage_mapping_data.csv).
Used in:
{% enddocs %}
