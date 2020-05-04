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
