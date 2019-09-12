{% docs create_snowplow_smau_events %}

This model encapsulates all activation events for stage create as defined in this gitlab issue. It reconciles 2 different data sources (Snowplow and Gitlab) with some common enabling us to calculate Daily/Monthly Active User count for this specific stage.

For more documentation on which event is tracked by each data source for this stage, refer to the 2 upstream models ((create_snowplow_smau_events)[https://gitlab-data.gitlab.io/analytics/dbt/snowflake/#!/model/model.gitlab_snowflake.create_snowplow_smau_events] and (create_gitlab_smau_events)[https://gitlab-data.gitlab.io/analytics/dbt/snowflake/#!/model/model.gitlab_snowflake.create_gitlab_smau_events])
 
{% enddocs %}
