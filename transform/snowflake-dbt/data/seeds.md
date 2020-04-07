{% docs gitlab_release_schedule_doc %}
This seed file capture the release date of each gitlab release.
Upcoming releases can be found [here](https://about.gitlab.com/upcoming-releases/).
Past releases can be found [here](https://about.gitlab.com/releases/).
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
