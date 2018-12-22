{% docs mrr_totals_levelled %}

This model extends `zuora_mrr_totals` by joining it to SFDC account data through `zuora_account`'s `crm_id` value.  From the account, we get the ultimate parent account information. It now holds details for _every_ level of analysis, including cohort months and cohort quarters for eafch level. 

{% enddocs %}
