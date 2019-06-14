{% docs netsuite_stitch_account_xf %}
Accounts in netsuite can have a parent/child relationship, and the child account_number can exist in the list of account_numbers creating duplication. 
To solve for this, the `accounts_xf` model joins an account to potential parents and creates a unique account number by combining both. 

The model currently assumes only one parent per child and only two levels, this model should be reviewed in the future to build a recursive strategy in case further levels are added in the future.
{% enddocs %}

{% docs netsuite_stitch_all_entries %}
This model unions the journal and non journal entries together into a single, explorable table. 
{% enddocs %}

{% docs netsuite_stitch_journal_entries %}
This model aggregates the journal entries from Netsuite.

This model will consolidate individual line items that match accounts and code into a single transaction. 

We filter to the relevant set of accounting codes. 5xxx are COGS (Cost of Good Sold) accounts and 6xxx are expense accounts. 5079 is intercompany transfers and should not be counted.
{% enddocs %}

{% docs netsuite_stitch_non_journal_entries %}
This model aggregates the non journal entries from Netsuite. 

This model will consolidate individual line items that match accounts and code into a single transaction. 

We filter to the relevant set of accounting codes. 5xxx are COGS (Cost of Good Sold) accounts and 6xxx are expense accounts. 5079 is intercompany transfers and should not be counted.
{% enddocs %}


{% docs netsuite_stitch_general_ledger %}

The General Ledger report (matching the one in the Netsuite UI) provides credits and debits for each account for a given period.

In the Netsuite UI General Ledger report all transactions pertaining to a journal entry are tallied up and allocated to debit/credit depending on the outcome, example:

```
Transaction ID: 123
  - Credit 100 to R&D for entity "Alex"
  - Debit 200 to Cost of Sales for entity "Taylor"
```

Will be represented in the Netsuite General Ledger as:

```
 Journal Entry:
  - Entity Name: Blank
  - 200 Debit
```

This model matches the Netsuite reporting, and will aggregate Journal entries dismissing department/entities in order to achieve this.

If reporting on individual line items for journal entires is required, please use the `all_entries` model.

{% enddocs %}



{% docs netsuite_stitch_entries_col_exchange_rate %}

In netsuite there are three currency rates to be mindful of:
- The currency rate of the transaction
- The base currency for the subsidiary where the transaction happened
- The base currency for the consolidated reporting (USD)

The exchange rate in the transaction goes from the transaction to the base currency.
`consolidated_exchange_rate` is the weighted average of conversion rates between the base currencies of subsidiaries. 

In example:

```
* Subsidiary B has a base currency of EURO
* Subsidiary A has a base currency of USD
* Transaction in A is done in Croatian Kuna
* Subsidiary B consolidates its revenue into parent-subsidiary A
```

The transaction will have an exchange rate of Kuna to EURO because that is the base of the subsidiary, so you have to do `transaction_amount`*`exchange_rate` to normalize to the subsidiary base currency

But if you want to consolidate to the parent-subsidiary you need normalized transaction * consolidated exchange rate.

{% enddocs %}

{% docs netsuite_stitch_entries_col_transaction_type %}
The type of transaction, can be one of: VendorBill, Check, PurchaseOrder, JournalEntry or VendorCredit
{% enddocs %}



{% docs netsuite_stitch_entries_col_transaction_date %}
The date of the transaction truncated to the day. 
{% enddocs %}


{% docs netsuite_stitch_entries_col_period_date %}
This represents the month the transaction occurred.
{% enddocs %}


{% docs netsuite_stitch_entries_col_subsidiary_id %}
Netsuite internal id for subsidiary.
{% enddocs %}


{% docs netsuite_stitch_entries_col_subsidiary_name %}
The subsidiary the transaction is tied to. Current options are: GitLab BV, GitLab GmbH, GitLab LTD, GitLab PTY LTD, Elimination Subsidiary.
GitLab Inc
{% enddocs %}


{% docs netsuite_stitch_entries_col_account_name %}
This is contains both the general ledger code and the name of what it represents. `6120 Marketing Programs : Demand Advertising` for example. 
{% enddocs %}


{% docs netsuite_stitch_entries_col_account_code %}
This is the 4 digit general code such as 6120 or 6031.  
**Important Note:** this number is *not* unique, as a child account can have the same code as an existing parent account, 
use ultimate_account if you want parents only, or unique_account_code if you need to operate at the child level
{% enddocs %}

{% docs netsuite_stitch_entries_col_unique_account_code %}
As Account Codes in netsuite have a parent/child relationship, this field will create a unique code by 
combining parent and child in the form of `parent:child` (or just child is there is no parent).
{% enddocs %}

{% docs netsuite_stitch_entries_col_ultimate_account_code %}
As Account Codes in netsuite have a parent/child relationship, this field will take the topmost level in the hirearchy,
i.e.: If there is no  `parent_account` it will be the account_code, if there is, then it will be the account_code of the parent.
{% enddocs %}

{% docs netsuite_stitch_entries_col_entity %}
This contains information about the who and what of the transaction. In the case of contractors, this data has been masked.
{% enddocs %}

{% docs netsuite_stitch_entries_col_entity_name %}
This contains information about the who and what of the transaction. In the case of contractors, this data has been masked.

Because Journal transactions are aggregates of multiple line items accross multiple entities, this field is blanked out in the General Ledger for Journal transactions.
If line-item entity information is required, use the `journal_entries` model.
{% enddocs %}

{% docs netsuite_stitch_entries_col_currency_name %}
The currency the original transaction occurred. 
{% enddocs %}


{% docs netsuite_stitch_entries_col_posting_period_id %}
Netsuite internal id for joining to Accounting Periods.
{% enddocs %}


{% docs netsuite_stitch_entries_col_department_name %}
High level department name such as G&A or Marketing.
{% enddocs %}


{% docs netsuite_stitch_entries_col_parent_deparment_name %}
The parent of the general department name if it has one.
{% enddocs %}


{% docs netsuite_stitch_entries_col_debit_amount %}
The amount debit for the transaction.
{% enddocs %}

{% docs netsuite_stitch_entries_col_gl_debit %}
The amount debit for the transaction aggregated at the transaction level. 

If the transaction contained both credits and debits, they will be compared and if `debit - credit > 0` then this result will be used, 
otherwise this field will be set to 0 and the transaction displayed as a credit.
{% enddocs %}


{% docs netsuite_stitch_entries_col_credit_amount %}
The amount credited for the transaction.
{% enddocs %}

{% docs netsuite_stitch_entries_col_gl_credit %}
The amount credit for the transaction aggregated at the transaction level. 

If the transaction contained both credits and debits, they will be compared and if `debit - credit < 0` then this result will be used, 
otherwise this field will be set to 0 and the transaction displayed as a debit.
{% enddocs %}


