{% docs snapshot_staging_table_updates %}
Overrides the built-in macro of the same name, in order to turn a critical inner join into a left join.
{% enddocs %}

{% docs snapshot_timestamp_with_deletes_strategy %}
This strategy is similar to the timestamp strategy, but adds support for records that are hard-deleted from the source. Only use this strategy if your snapshot select statement is pulling current records only, which is not the default for PGP sources. RAW does not drop historical PGP rows after they've been deleted, so they need to be explicitly filtered out. 
{% enddocs %}