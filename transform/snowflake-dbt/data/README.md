### Version Usage Stats to Stage Mapping
The version_usage_stats_to_stage_mapping_data.csv maps usage ping fields to different value and team stages. See https://about.gitlab.com/handbook/product/categories/#hierarchy for more information about stages. If the stage is `ignored` it was determined that they are not part of any stage or there is no relevant data. See https://gitlab.com/gitlab-org/telemetry/issues/18 for more context.

### Zuora Excluded Accounts
Zuora Accounts added here will be excluded from all relevant Zuora base models. The is_permanently_excluded column designates whether the column should be permantely excluded or just temporarily. The description column is a non functional helper for us to track which accounts are excluded.