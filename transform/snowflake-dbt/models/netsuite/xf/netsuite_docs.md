{% docs netsuite_journal_entries %}
This model aggregates the journal entries from Netsuite. Per the GitLab finance team, ti was found that Netsuite takes the weighted average of the exchange rate to generate the consolidated income statement. This is in contrast to taking the exchanged rate based on the current value.

This model will consolidate individual line items that match accounts and code into a single transaction. 

We filter to the relevant set of accounting codes. 5xxx are COGS (Cost of Good Sold) accounts and 6xxx are expense accounts. 5079 is intercompany transfers and should not be counted.
{% enddocs %}


{% docs netsuite_non_journal_entries %}
This model aggregates the non journal entries from Netsuite. Per the GitLab finance team, ti was found that Netsuite takes the weighted average of the exchange rate to generate the consolidated income statement. This is in contrast to taking the exchanged rate based on the current value.

This model will consolidate individual line items that match accounts and code into a single transaction. 

We filter to the relevant set of accounting codes. 5xxx are COGS (Cost of Good Sold) accounts and 6xxx are expense accounts. 5079 is intercompany transfers and should not be counted.
{% enddocs %}


{% docs netsuite_general_ledger %}
This model unions the journal and non journal entries together into a single, explorable table. 
{% enddocs %}


{% docs netsuite_entries_col_transaction_type %}
Current transaction types are: _check, _vendorCredit, _journal, _vendorBill, and null.
{% enddocs %}


{% docs netsuite_entries_col_transaction_date %}
The date of the transaction truncated to the day. 
{% enddocs %}


{% docs netsuite_entries_col_period_date %}
This represents the month the transaction occurred.
{% enddocs %}


{% docs netsuite_entries_col_subsidiary_id %}
Netsuite internal id for subsidiary.
{% enddocs %}


{% docs netsuite_entries_col_subsidiary_name %}
The subsidiary the transaction is tied to. Current options are: GitLab BV, GitLab GmbH, GitLab LTD, GitLab PTY LTD, Elimination Subsidiary.
GitLab Inc
{% enddocs %}


{% docs netsuite_entries_col_exchange_rate %}
The exchange rate of the transaction.
{% enddocs %}


{% docs netsuite_entries_col_account_name %}
This is contains both the general ledger code and the name of what it represents. `6120 Marketing Programs : Demand Advertising` for example. 
{% enddocs %}


{% docs netsuite_entries_col_account_code %}
This is the 4 digit general code such as 6120 or 6031.
{% enddocs %}


{% docs netsuite_entries_col_entity %}
This contains information about the who and what of the transaction. In the case of contractors, this data has been masked.
{% enddocs %}


{% docs netsuite_entries_col_currency_name %}
The currency the original transaction occurred. 
{% enddocs %}


{% docs netsuite_entries_col_posting_period_id %}
Netsuite internal id for joining to Accounting Periods.
{% enddocs %}


{% docs netsuite_entries_col_department_name %}
High level department name such as G&A or Marketing.
{% enddocs %}


{% docs netsuite_entries_col_parent_deparment_name %}
The parent of the general department name if it has one.
{% enddocs %}


{% docs netsuite_entries_col_debit_amount %}
The amount debit for the transaction.
{% enddocs %}


{% docs netsuite_entries_col_credit_amount %}
The amount credited for the transaction.
{% enddocs %}


{% docs netsuite_entries_col_consolidated_exchange %}
The consolidated exchange rate which is used to match what we see in the Netsuite UI.
{% enddocs %}

