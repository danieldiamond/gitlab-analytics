{% docs customers_db_orders_with_valid_charges_data %}

This is an intermediate ephemeral model used in the `customers_db_charges_xf` model. Each row is a different zuora rate_plan_charge with a unique `rate_plan_charge_id` key.

We isolate from the `customers_db_orders_snapshots` table the orders with the following criteria:

* `subscription_name_slugify` is not NULL
* `product_rate_plan_id` is not NULL
* `order_is_trial` is FALSE 

This will help us identify paid orders that were active when we started recording snapshots (Sept. 2019). From this, we will join them to zuora tables with the following keys:

* `subscription_name_slugify` joins to `zuora_subscription_xf`
* `product_rate_plan_id` joins to `zuora_rate_plan`

One weird behavior of the `customers_db_orders` table is that a subscription (unique `subscription_name_slugify`) can be linked to several customer accounts (`customer_id`) and therefore we can find several `customer_id` for the same `subscription_name_slugify`. We then capture the following metadata: 

* `current_customer_id` which is the `customer_id` linked to the latest updated order in the `customers_db_orders` table 
* `first_customer_id` which is the `customer_id` linked to the oldest (oldest `order_created_at`) order in the `customers_db_orders` table
* `customer_id_list`: all customers that are linked to a specific subscription.  


During the life of an order, the customer can change the namespace attached to the subscription. Using snapshots helps us record these changes. We capture the following 2 fields: 

* `current_gitlab_namespace_id` which is the `gitlab_namespace_id` linked to the latest updated order in the `customers_db_orders` table 
* `first_gitlab_namespace_id` which is the `gitlab_namespace_id` linked to the oldest (oldest `order_created_at`) order in the `customers_db_orders` table
* `gitlab_namespace_id_list`: all gitlab_namespace that are linked to a specific subscription.


{% enddocs %}

{% docs customers_db_orders_with_incomplete_charges_data %}

This is an intermediate ephemeral model used in the `customers_db_charges_xf` model. Each row is a different zuora rate_plan_charge with a unique `rate_plan_charge_id` key.

As described in the `customers_db_orders` documentation, this base table has some weird overriding logics. When a subscription expires, the orders associated to this subscription lose some key important metadata: `product_rate_plan_id`.

Example:

| ORDER_ID | CUSTOMER_ID | SUBSCRIPTION_NAME_SLUGIFY | ORDER_IS_TRIAL | GITLAB_NAMESPACE_ID | ORDER_START_DATE        | ORDER_END_DATE          | PRODUCT_RATE_PLAN_ID             |
|----------|-------------|---------------------------|----------------|---------------------|-------------------------|-------------------------|----------------------------------|
| 5336     | 9010        | a-s00007947               | FALSE          | 2542496             | 2018-03-02 00:00:00.000 | 2019-03-02 00:00:00.000 |   

That means that all these expired subscriptions can't be captured and joined in a clean way to zuora models (we both join on `subscription_name_slugify` and `product_rate_plan_id`). 

This model isolates these subscriptions and will join them to zuora tables only through the `subscription_name_slugify` key.

Weird behavior of the `customers_db_orders` tables are that a subscription (unique `subscription_name_slugify`) can be linked to several customer accounts (`customer_id`) and therefore we can find several `customer_id` for the same `subscription_name_slugify`. We then capture the following metadata: 

* `current_customer_id` which is the `customer_id` linked to the latest updated order in the `customers_db_orders` table 
* `first_customer_id` which is the `customer_id` linked to the oldest (oldest `order_created_at`) order in the `customers_db_orders` table
* `customer_id_list`: all customers that are linked to a specific subscription.  


During the life of an order, the customer can change the namespace attached to the subscription. Using snapshots helps us record these changes. We capture the following 2 fields: 

* `current_gitlab_namespace_id` which is the `gitlab_namespace_id` linked to the latest updated order in the `customers_db_orders` table 
* `first_gitlab_namespace_id` which is the `gitlab_namespace_id` linked to the oldest (oldest `order_created_at`) order in the `customers_db_orders` table
* `gitlab_namespace_id_list`: all gitlab_namespace that are linked to a specific subscription.

We finally join with the `customers_db_orders_with_valid_charges` in order to exclude the `rate_plan_charge_id` that are already in this model.

{% enddocs %}
