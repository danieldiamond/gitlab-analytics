{% docs customers_db_charges_xf %}

This model creates a clean table that one can easily join on `zuora_base_mrr` tables to have some financial information. It deduplicates, transforms and joins on `zuora_subscription_xf`, `zuora_rate_plan` and `zuora_rate_plan_charge` in order to create a table at the granularity of one charge per row.

This model first unions the 2 ephemeral model `customers_db_charges_with_valid_charges` and `customers_db_charges_with_incomplete_charges` which provide a clean list of all orders that have been created in the subscription portal and that can be linked to zuora subscriptions and charges.

The union of these 2 tables is then easily joined on `zuora_base_mrr` to add financial information such as `mrr`, `tcv`, `quantity_ordered`... 

{% enddocs %}

{% docs customers_db_trials %}

This model collects all trials started from the subscription portal. For this we use the `customers_db_orders_snapshots_base` model in order to isolate them. This model does the following thing:

* It isolates the orders that are flagged with the column `is_trial=TRUE`
* It deduplicates by taking the first row created
* It joins with customers, users and namespaces. 

The `customers_db_orders_snapshots_base` model has reliable data from the 1st of September, therefore we select only orders that have a `start_date` after this date.

Examples:

| ORDER_ID | ORDER_UPDATED_AT        | ORDER_START_DATE  | ORDER_END_DATE | ORDER_IS_TRIAL | SUBSCRIPTION_NAME_SLUGIFY |
|----------|-------------------------|-------------------|----------------|----------------|---------------------------|
| 32177    | 2019-09-06 23:09:21.858 | 2019-08-17        | 2019-09-15     | TRUE           |                           |
| 32177    | 2019-09-13 22:39:18.916 | 2019-08-17        | 2019-09-27     | TRUE           |                           |
| 32177    | 2019-09-26 21:26:23.227 | 2019-08-17        | 2019-10-02     | TRUE           |                           |
| 32177    | 2019-10-02 16:32:45.664 | 2019-10-02        | 2019-10-04     | TRUE           |                           |
| 32177    | 2019-10-02 00:00:00.075 | 2019-10-02        |                | FALSE          |                           |
| 32177    | 2019-10-03 20:11:31.497 | 2019-10-02        | 2020-10-02     | FALSE          | order-1-name-gold         |

NB: subscription_name_slugify has been anonymised

This order examplifies perfectly what is happening in the table `customers_db_orders`. When the order starts, 17th Aug, 2019, it is a trial. That means that the flag `order_is_trial` is set to TRUE. But it doesn't have either a subscription_id or a subscription_name (`subscription_name_slugify` is null). When it converts, 2nd Nov, 2019, the `order_is_trial` flag is set to `FALSE`, the order_start_date (and order_end_date) is changed and a `subscription_name` and `subscription_id` are set! (last row of the table)


{% enddocs %}
