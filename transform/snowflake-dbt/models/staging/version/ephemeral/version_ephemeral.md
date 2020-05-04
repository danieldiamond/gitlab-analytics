{% docs version_usage_data_unpacked %}

This model transforms the `version_usage_data_unpacked_intermediate` by turning for all counter columns the `-1` (which happen when the queries time out) into `NULL`. This transformation prevents us from displaying negative data for these usage counters.  

{% enddocs %}

{% docs version_user_activity_by_stage_monthly_unpacked %}

This model flattens the JSON contained in the column `usage_activity_by_stage` twice to build a table that shows for a specific usage_ping `id` and a specific `usage_action_name`, the monthly_count (count of unique users who performed this action in the last 28 days from the `created_at` timestamp)

The JSON looks typically like that

```
"usage_activity_by_stage": {
"configure": {
  "clusters_disabled": 9048,
  "clusters_enabled": 92343
},
"create": {
  "merge_requests": 977856,
  "snippets": 2434
},
"manage": {
  "events": 6976879
}
}
```

and will be transformed into a table that looks like that:

| usage_action_name  | stage_name | usage_action_count|
|--------------------|-----------|---------|
| clusters\_disabled | configure | 9048    |
| clusters\_enabled  | configure | 92343   |
| merge\_requests    | create    | 977856  |
| snippets           | create    | 2434    |
| events             | manage    | 6976879 |


This table will be used to create calculations for MAU and SMAU KPIs.

{% enddocs %}

{% docs version_usage_data_weekly_opt_in_summary %}

This model summarizes which instances from the licenses app successfully send a usage ping at a weekly granularity.  
Only self-managaged instances that have a listed license file in the license app are included in this model. Trials are excluded entirely.  
Instances are included in this analysis for any week where the Monday falls between their "license start date" and "license expires date".  

Example query usage:
```sql
SELECT
  week,
  AVG(did_send_usage_data::INTEGER)
FROM analytics.version_usage_data_weekly_opt_in_summary
GROUP BY 1
ORDER BY 1 DESC
```

{% enddocs %}
