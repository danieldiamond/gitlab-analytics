{% docs snapshot_staging_table_updates %}
Overrides the built-in macro of the same name, in order to turn a critical inner join into a left join.
{% enddocs %}

{% docs snapshot_timestamp_with_deletes_strategy %}
Only use if your snapshot select statement is pulling current records only. This is not the default for PGP sources, which store historical data that has since been deleted.
{% enddocs %}