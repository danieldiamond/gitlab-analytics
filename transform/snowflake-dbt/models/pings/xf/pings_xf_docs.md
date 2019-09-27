{% docs pings_list %}

GitLab has been sending a weekly payload containing usage data from self-managed instances which haven't opted out. This weekly payload have changed structure over time. Some usage pings (understand metrics) have been added and we kept on switching the structure (sometimes nesting some JSONs and some other time un-nesting them).

This model creates a comprehensive list of all pings that have been used with 2 different columns:

* `ping_name`: the raw extracted path  in the JSON. In example A that would be `EXAMPLE`. In example B, `EXAMPLE`
* `full_ping_name`: cleaned `ping_name`. Example A and example B will have the same `full_ping_name`. We will use the `full_ping_name` to create downstream clean models


{% enddocs %}

{% docs pings_usage_data_monthly_change_by_stage %}

Monthly changes for usage statistics based on the cumulative monthly ping data, summarized into stages as per [ping_metrics_to_stage_mapping_data.csv](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/data/ping_metrics_to_stage_mapping_data.csv).

The following macros are used:
* stage_mapping - finds the specific columns to aggregate for each stage

{% enddocs %}

{% docs pings_usage_data_monthly_change %}

Monthly changes for usage statistics based on the cumulative monthly ping data.

Usage statistics are based on cumulative numbers, this means most of the statistics counted up since the installation. Many counts highlight usage like issues created, while there are other counts that show the given feature has been enabled or not.
The main goal for this dataset is to highlight usage in the given month. To achieve this, calculates monthly differences or flag if a feature has been used already so we can assume it's still in use.

The following macros are used:

* monthly_change - Using the change suffix, calculating differences for each consecutive usage ping by uuid
* monthly_is_used - Adding the is_used suffix, keep the counts that show the given feature has been enabled or not
{% enddocs %}

{% docs pings_usage_data_unpacked %}

Example of stats_used format (useful for examples after):

EXAMPLE A
```json
{
   "operations_dashboard_users_with_projects_added":2
}
```

EXAMPLE B
```json
{
   "operations_dashboard.users_with_projects_added":5
}
```
 
The model unpacks the pings usage data stored in json column `stats_used` in model `pings_usage_data`. To do so, we perform the following actions:

* flatten the `stats_used` json. The flattening explodes key-value pairs in the JSON into multiple rows. From each pair, we create 3 columns:
  * ping_name: The key of the key-value pair. In the example A above, for the first pair the key is `operations_dashboard_users_with_projects_added`. For example B `operations_dashboard.users_with_projects_added`. These are the 
  * full_ping_name: it replicates what is done in `pings_list` model. It takes the key and replaces the `.` to `_`. For both our examples, full_ping_name would be `operations_dashboard_users_with_projects_added`. The full_ping_name are going to be the columns of our model.
  * ping_value: the value of the key-value pair. In the example, it is . In our model this is the value of the metrics we calculate 
  
* pivot manually the CTE created above. The `pings_list` gives us the full list of columns that need to be in the final table. We iterate through each element of the list (each value of `full_ping_name` column in `pings_list` model) and find the MAX of `ping_value` (actually we can get only one row with a `ping_value` not null).

{% enddocs %}
