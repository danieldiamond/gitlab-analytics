{% docs sfdc_accounts_xf %}

This table contains information about individual accounts (organizations and persons), their sales segmentation, and parentage. 

Fields prefixed with TSP_ are related to [Territory Success Planning](https://about.gitlab.com/handbook/sales/field-operations/sales-operations/go-to-market/#territory-success-planning-tsp) and intended as staging fields updated from a variety of data sources, and at given intervals copied over to the "Actual" set of fields for general use. The relationship between "Actual" and TSP fields can be found [here](https://about.gitlab.com/handbook/sales/field-operations/sales-systems/gtm-technical-documentation/). Note that these fields are defined in Salesforce and brought into the data warehouse as-is.

{% enddocs %}

{% docs sfdc_lead_xf %}

This table contains information about leads, who are prospects or potential Opportunities.

Fields prefixed with TSP_ are related to [Territory Success Planning](https://about.gitlab.com/handbook/sales/field-operations/sales-operations/go-to-market/#territory-success-planning-tsp) and intended as staging fields updated from a variety of data sources, and at given intervals copied over to the "Actual" set of fields for general use. The relationship between "Actual" and TSP fields can be found [here](https://about.gitlab.com/handbook/sales/field-operations/sales-systems/gtm-technical-documentation/). Note that these fields are defined in Salesforce and brought into the data warehouse as-is.

{% enddocs %}

{% docs sfdc_opportunity_field_historical %}

This model transforms sfdc_opportunity_field_history to construct the history of changes to opportunities prior to 2019-10-01 in the same structure as sfdc_opportunity_snapshot_history.

For each opportunity_id, the state of each field is taken from sfdc_opportunity_snapshot_history at 2019-10-01. The value for each field is backfilled until a corresponding change in sfdc_opportunity_field_history is found, or until the created date of the opportunity in the event of no changes to the field history.

NOTE: Only opportunities that were not deleted (see [Salesforce Documentation](https://help.salesforce.com/articleView?id=home_delete.htm&type=5)) as of 2019-10-01 are included in this model. Hard deleted opportunities are not present in current extracts of opportunity and opportunityfieldhistory, so there is no mechanism for constructing historical records for these opportunities.

{% enddocs %}

{% docs sfdc_opportunity_stage_duration %}

This table provides the days_in_stage where the value is the greater of the aggregated days in stage or .0001 if the days in stage sums up to 0.

The calculation is done in this manner so that the average is taken out of all opportunities (including those with a value of 0).

{% enddocs %}

{% docs sfdc_reason_for_loss_unpacked %}

This model unpackes the reason for loss field for easier querying. 

NOTE: This creates a fan out and raw counts of opportunities will be inflated.

{% enddocs %}