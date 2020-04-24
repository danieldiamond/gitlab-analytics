{% docs version_usage_data_action_summary_xf %}

This model unnests the JSON in the `usage_data.usage_activity_by_stage` column. The transformed model has one row per usage ping per **action** (which roll up into stages).

{% enddocs %}


{% docs version_usage_stats_list %}

GitLab has been sending a weekly payload containing usage data from self-managed instances which haven't opted out. This weekly payload have changed structure over time. Some usage pings (understand metrics) have been added and we kept on switching the structure (sometimes nesting some JSONs and some other time un-nesting them).

This model creates a comprehensive list of all usage ping stats that have been used with 2 different columns:

* `ping_name`: the raw extracted path  in the JSON. In example A that would be `EXAMPLE`. In example B, `EXAMPLE`
* `full_ping_name`: cleaned `ping_name`. Example A and example B will have the same `full_ping_name`. We will use the `full_ping_name` to create downstream clean models


{% enddocs %}

{% docs version_usage_data_monthly_change %}

Monthly changes for usage statistics based on the cumulative monthly ping data.

Usage statistics are based on cumulative numbers, this means most of the statistics counted up since the installation. Many counts highlight usage like issues created, while there are other counts that show the given feature has been enabled or not.
The main goal for this dataset is to highlight usage in the given month. To achieve this, calculates monthly differences or flag if a feature has been used already so we can assume it's still in use.

The following macros are used:

* monthly_change - Using the change suffix, calculating differences for each consecutive usage ping by uuid
* monthly_is_used - Adding the is_used suffix, keep the counts that show the given feature has been enabled or not
{% enddocs %}

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
