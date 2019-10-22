{% docs customers_db_trial_started %}

This model collects all trials started from the subscription portal. For this we use the `customers_db_orders_snapshots_base` model in order to isolate them. This model does the following thing:

* It isolates the orders that are flagged with the column `is_trial=TRUE`
* It deduplicates by taking the first row created
* It joins with customers, users and namespaces. 

The `customers_db_orders_snapshots_base` model has reliable data from the XXth of October, therefore we select only orders that have a `start_date` after this date.

{% enddocs %}
