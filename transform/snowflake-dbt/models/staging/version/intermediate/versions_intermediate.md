{% docs version_usage_data_unpacked_intermediate %}

Example of stats_used format (useful examples for after):

EXAMPLE A (not nested json)
```json
{
   "ping_key_with_detail": 2,
   "other_metrics": "other_values"
}
```

EXAMPLE B (nested json)
```json
{
  "ping_key": { 
     "with_detail": 5
  },
  "other_metrics": "other_values"
}
```
 
This model unpacks the usage ping JSON data stored in the json-type column `stats_used` in the model `version_usage_data`. To do so, we perform the following actions:

1. Flatten the `stats_used` JSON. The flattening unnests all key/value pairs into multiple rows (one row per pair). From each pair, we create 3 columns:

  * ping_name: The path to the pair. Note that nested keys are joined with `.`
  * full_ping_name: This replicates what is done in `pings_list` model where `.` is replaced with `_`
  * ping_value: the value of the pair
  
| Example | ping_name | full_ping_name | ping_value |
|---|---|---|---|
| A | `ping_key_with_detail` | `ping_key_with_detail` | 2 |
| B | `ping_key.with_detail` | `ping_key_with_detail` | 8 |

1. The `version_usage_stats_list` model gives us the full list of columns that need to be in the final table. We iterate through each element of the list (i.e. each value of `full_ping_name` column in `pings_list` model) and find the MAX of `ping_value`.

This model also adds some information about the related Zuora subscription, including its status and CRM ID.

{% enddocs %}
