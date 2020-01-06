{% docs sfdc_accounts_xf %}

This table contains information about individual accounts (organizations and persons), their sales segmentation, and parentage. 

Fields prefixed with TSP_ are related to [Territory Success Planning](https://about.gitlab.com/handbook/sales/field-operations/sales-operations/go-to-market/#territory-success-planning-tsp) and intended as staging fields updated from a variety of data sources, and at given intervals copied over to the "Actual" set of fields for general use. The relationship between "Actual" and TSP fields can be found [here](https://about.gitlab.com/handbook/sales/field-operations/sales-systems/gtm-technical-documentation/). Note that these fields are defined in Salesforce and brought into the data warehouse as-is.

{% enddocs %}

{% docs sfdc_accounts_xf_col_ultimate_parent_account_id %}

Salesforce Account ID for Ultimate Parent Account

{% enddocs %}

{% docs sfdc_accounts_xf_col_ultimate_parent_account_name %}

Salesforce Account Name for Ultimate Parent Account

{% enddocs %}


{% docs sfdc_opportunity_stage_duration %}

This table provides the days_in_stage where the value is the greater of the aggregated days in stage or .0001 if the days in stage sums up to 0.

The calculation is done in this manner so that the average is taken out of all opportunities (including those with a value of 0).

{% enddocs %}
