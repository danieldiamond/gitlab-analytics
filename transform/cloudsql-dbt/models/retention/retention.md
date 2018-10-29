{% docs zuora_mrr_totals_levelled %}

This model extends `zuora_mrr_totals` by joining it to SFDC account data through `zuora_account`'s `crm_id` value.  From the account, we get the ultimate parent account information. It now holds details for _every_ level of analysis, including cohort months and cohort quarters for eafch level. 

{% enddocs %}

{% docs parent_account_mrr %}

This model filters out accounts without ultimate parent accounts from `zuora_mrr_totals_levelled` (a data quality issue that the finance team is actively working to solve). At this point, we drop all subscription-level information and reorganize the data to be at the parent level, including aggregating MRR and recalculating MRR 12 months ago. 

{% enddocs %}

{% docs parent_account_mrr_agg %}

This model aggregates net retention and gross retention by month. It was separated from `parent_account_mrr` because Looker doesn't support conditonal filtering on measures, which is needed since we filter out accounts that did not exist a year ago before aggregating. 

{% enddocs %}