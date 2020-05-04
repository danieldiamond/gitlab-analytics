{% docs version_usage_data_unpacked %}

This model transforms the `version_usage_data_unpacked_intermediate` by turning for all counter columns the `-1` (which happen when the queries time out) into `NULL`. This transformation prevents us from displaying negative data for these usage counters.  

{% enddocs %}


{% docs version_usage_data_monthly_change %}

Monthly changes for usage statistics based on the cumulative monthly ping data.

Usage statistics are based on cumulative numbers, this means most of the statistics counted up since the installation. Many counts highlight usage like issues created, while there are other counts that show the given feature has been enabled or not.
The main goal for this dataset is to highlight usage in the given month. To achieve this, calculates monthly differences or flag if a feature has been used already so we can assume it's still in use.

The following macros are used:

* monthly_change - Using the change suffix, calculating differences for each consecutive usage ping by uuid
* monthly_is_used - Adding the is_used suffix, keep the counts that show the given feature has been enabled or not
{% enddocs %}
