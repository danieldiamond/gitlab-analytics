{% docs customers_db_orders_with_valid_charges %}

We isolate from the `customers_db_orders_snapshots` table the orders with the following criteria:

* `subscription_name_slugify` is not NULL
* `product_rate_plan_id` is not NULL
* `order_is_trial` is FALSE 

This will help us identify paid orders that were active when we started recording snapshots (Sept. 2019). From this, we will join them to zuora tables with the following keys:

* `subscription_name_slugify` joins to `zuora_subscription_xf`
* `product_rate_plan_id` joins to `zuora_rate_plan`

Weird behavior of the `customers_db_orders` tables are that a subscription (unique `subscription_name_slugify`) can be linked to several customer accounts (`customer_id`) and therefore we can find several `customer_id` for the same `subscription_name_slugify`. We then capture the following metadata: 

* `current_customer_id` which is the `customer_id` linked to the latest updated order in the `customers_db_orders` table 
* `first_customer_id` which is the `customer_id` linked to the oldest (oldest `order_created_at`) order in the `customers_db_orders` table
* `customer_id_list`: all customers that are linked to a specific subscription.  


During the life of an order, the customer can change the namespace attached to the subscription. Using snapshots helps us record these changes. We capture the following 2 fields: 

* `current_gitlab_namespace_id` which is the `gitlab_namespace_id` linked to the latest updated order in the `customers_db_orders` table 
* `first_gitlab_namespace_id` which is the `gitlab_namespace_id` linked to the oldest (oldest `order_created_at`) order in the `customers_db_orders` table
* `gitlab_namespace_id_list`: all gitlab_namespace that are linked to a specific subscription.


{% enddocs %}

{% docs customers_db_orders_with_valid_charges %}

This is an intermediate ephemeral model used in the `customers_db_charges_xf`.

As described in the `customers_db_orders` documentation, this base table has some weird overriding logics. When a subscription expires, the orders associated to this subscription lose some key important metadata: `product_rate_plan_id`.

Example:



That means that all these expired subscriptions can't be captured and joined in a clean way to zuora models (we both join on `subscription_name_slugify` and `product_rate_plan_id`). 

This model isolates these subscriptions and will join them to zuora tables only through the `subscription_name_slugify` key.
