{% docs blacklisted_uuid %}
This seed file captures the UUID sending us abnormal usage ping counters. Those UUID are excluded from the analytics downstream models in order to allow easy charting.
For example in [this issue](https://gitlab.com/gitlab-data/analytics/-/issues/4343), one can clearly see an abnormal spike in monthly numbers of `projects_prometheus_active`.
{% enddocs %}

{% docs gitlab_release_schedule_doc %}
This seed file capture the release date of each minor (X.y) gitlab release.
Upcoming releases can be found [here](https://about.gitlab.com/upcoming-releases/).  
Past releases can be found [here](https://about.gitlab.com/releases/).  
Historical release dates were copied from [this issue](https://gitlab.com/gitlab-com/www-gitlab-com/issues/5396).
{% enddocs %}

{% docs version_usage_stats_to_stage_mappings_doc %}
The version_usage_stats_to_stage_mapping_data.csv maps usage ping fields to different value and team stages. See https://about.gitlab.com/handbook/product/categories/#hierarchy for more information about stages. If the stage is `ignored` it was determined that they are not part of any stage or there is no relevant data. See https://gitlab.com/gitlab-org/telemetry/issues/18 for more context.
{% enddocs %}

{% docs zuora_excluded_accounts_doc %}
## [Zuora Excluded Accounts](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/data/zuora_excluded_accounts.csv)
Zuora Accounts added here will be excluded from all relevant Zuora base models.
* The `is_permanently_excluded` column is non-functional and designates whether the column should be permanently excluded or just temporarily.
* The `description` column is a non-functional helper for us to track which accounts are excluded.
{% enddocs %}
