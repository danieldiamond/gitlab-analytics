{% docs sfdc_reason_for_loss_unpacked %}

This model unpackes the reason for loss field for easier querying. 

NOTE: This creates a fan out and raw counts of opportunities will be inflated.

{% enddocs %}

{% docs sfdc_opportunity_field_historical %}

This model transforms sfdc_opportunity_field_history to construct the history of changes to opportunities prior to 2019-10-01 in the same structure as sfdc_opportunity_snapshot_history.

For each opportunity_id, the state of each field is taken from sfdc_opportunity_snapshot_history at 2019-10-01. The value for each field is backfilled until a corresponding change in sfdc_opportunity_field_history is found, or until the created date of the opportunity in the event of no changes to the field history.

NOTE: Only opportunities that were not deleted (see [Salesforce Documentation](https://help.salesforce.com/articleView?id=home_delete.htm&type=5)) as of 2019-10-01 are included in this model. Hard deleted opportunities are not present in current extracts of opportunity and opportunityfieldhistory, so there is no mechanism for constructing historical records for these opportunities.

{% enddocs %}