{% docs customers_db_subscriptions %}

This model creates a clean table that one can easily join on `zuora_base_mrr` tables to have some financial information. It deduplicates, transforms and joins on `zuora_subscription_xf`, `zuorate_rate_plan` and `zuora_rate_plan_charge` in order to create a table at the granularity of one charge per row.

On the zuora side, the model does exactly the same transformation as the [`zuora_base_mrr` model](LINK) in order to filter out only valid charges for mrr calculations.

On the customers side, we add several important pieces of information about the subscriptions:

* customers: 
  * `current_customer_id` which is the `customer_id` linked to the latest updated order in the `customers_db_orders` table 
  * `first_customer_id` which is the `customer_id` linked to the oldest (oldest `order_created_at`) order in the `customers_db_orders` table
  * `customer_id_list`: all customers that are linked to a specific subscription.  
* gitlab namespaces: 
  * `current_gitlab_namespace_id` which is the `gitlab_namespace_id` linked to the latest updated order in the `customers_db_orders` table 
  * `first_gitlab_namespace_id` which is the `gitlab_namespace_id` linked to the oldest (oldest `order_created_at`) order in the `customers_db_orders` table
  * `gitlab_namespace_id_list`: all gitlab_namespace that are linked to a specific subscription.

{% enddocs %}

{% docs customers_db_trials %}

This model collects all trials started from the subscription portal. For this we use the `customers_db_orders_snapshots_base` model in order to isolate them. This model does the following thing:

* It isolates the orders that are flagged with the column `is_trial=TRUE`
* It deduplicates by taking the first row created
* It joins with customers, users and namespaces. 

The `customers_db_orders_snapshots_base` model has reliable data from the 1st of September, therefore we select only orders that have a `start_date` after this date.

Examples:



{% enddocs %}
