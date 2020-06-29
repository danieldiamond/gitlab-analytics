{% docs customers_db_orders %}

customers_db_orders is the base model built upon the orders table in the customers_db table. This table is a list of the current state of all orders that have been completed through the subscription portal.

This table is a weird, complicated tables with a lot of strange mechanisms. Below, you will find a list of all the different findings (results of huge data exploration led by @ekastalein and @mpeychet in October 2019):

### What is an order ?

An order is a specific subscription to a specific GitLab set of product categories (called delivery, and being either SaaS or Self-Managed). When this subscription comes to an end, regardless if the subscription is renewed, expired or converted to a paid subscription (if the order has been initiated as a trial), the order is updated.

Some examples of orders:

#### trial 

| ORDER_ID | CUSTOMER_ID | SUBSCRIPTION_NAME_SLUGIFY | ORDER_IS_TRIAL | GITLAB_NAMESPACE_ID | ORDER_START_DATE        | ORDER_END_DATE          |
|----------|-------------|---------------------------|----------------|---------------------|-------------------------|-------------------------|
| 41362    | 133208      |                           | TRUE           | 6441478             | 2019-11-04 00:00:00.000 | 2019-12-04 00:00:00.000 |
| 41359    | 133204      |                           | TRUE           | 6441349             | 2019-11-04 00:00:00.000 | 2019-12-04 00:00:00.000 |

order is created, without `subscription_name_slugify`. Order duration is one month.

When a trial expires, `order_is_trial` turns to `FALSE`. This example is taken from `customers_db_orders_snapshots` to see how the data is captured and overwritten.

| ORDER_ID | CUSTOMER_ID | SUBSCRIPTION_NAME_SLUGIFY | ORDER_IS_TRIAL | GITLAB_NAMESPACE_ID | ORDER_START_DATE        | ORDER_END_DATE          |
|----------|-------------|---------------------------|----------------|---------------------|-------------------------|-------------------------|
| 38423    | 98873       |                           | FALSE          | 5912206             | 2019-04-03 00:00:00.000 |                         |
| 38423    | 98873       |                           | TRUE           | 5912206             | 2019-03-04 00:00:00.000 | 2019-04-03 00:00:00.000 |

`order_start_date` is now the date when the trial expired and the subscription turns to free. No end-date 

#### paid and currently active subscription 

| ORDER_ID | CUSTOMER_ID | SUBSCRIPTION_NAME_SLUGIFY | ORDER_IS_TRIAL | GITLAB_NAMESPACE_ID | ORDER_START_DATE        | ORDER_END_DATE          | product_rate_plan_id          |
|----------|-------------|---------------------------|----------------|---------------------|-------------------------|-------------------------|-------------------------------------| 
| 41356    | 132982      | sub1                      | FALSE          | 6430883             | 2019-01-03 00:00:00.000 | 2020-01-03 00:00:00.000 | 2c92a0ff63afe3e40163da7e174a20ee |
| 41355    | 133196      | sub2                      | FALSE          | 6440904             | 2019-04-03 00:00:00.000 | 2020-04-03 00:00:00.000 | 2c92a0ff5a840412015aa3cde86f2ba6 |
| 41351    | 133191      | sub3                      | FALSE          | 6440538             | 2018-08-03 00:00:00.000 | 2019-08-03 00:00:00.000 | 2c92a0ff6145d07001614efff26d15da |

`subscription_name_slugify` is anonymised in this example. `gitlab_names`

#### expired subscription 

Currently, the downgrade process seems quite unstable. For both SaaS and self-managed plans, we see 2 different behavior. The `product_rate_plan_id` should turn to `NULL` once the order is expired, but it seems that a lot of orders are not correctly expired and keep having the `product_rate_plan_id` set even though the order expired. While it shouldn't be a problem for self-managed product categories, it is a currently a problem for SaaS product categories, customers with expired subscription still having access to paid plans with their namespace.

Some examples:

| ORDER_ID | CUSTOMER_ID | SUBSCRIPTION_NAME_SLUGIFY | ORDER_IS_TRIAL | GITLAB_NAMESPACE_ID | ORDER_START_DATE        | ORDER_END_DATE          | PRODUCT_RATE_PLAN_ID             |
|----------|-------------|---------------------------|----------------|---------------------|-------------------------|-------------------------|----------------------------------|
| 6259     | 10075       | a-s00008638               | FALSE          | 2756798             | 2018-04-16 00:00:00.000 | 2019-04-16 00:00:00.000 | 2c92a0ff5a840412015aa3cde86f2ba6 |
| 5336     | 9010        | a-s00007947               | FALSE          | 2542496             | 2018-03-02 00:00:00.000 | 2019-03-02 00:00:00.000 |                      |

First case is expired but not correctly downgraded. The 2nd is expired and currently downgraded.

#### Problems raised

The problem with this update mechanism is that we totally lose the data about the history of this order. When an order is expired, for example, we can't see in this table if the order was a trial, SaaS subscription...

For example, that means that looking at this table, we are not able to calculate the number of trials that have been started in August 2019 (they have all expired and now look like expired subscription)

We can't know either how many Gold Subscription have been started in 2018...

Another example is some subscriptions that have been downgraded from Silver to Bronze. Looking at the table, we will see only the latest subscription, the Bronze. No possibility to trace back to the Silver one without any join!

Some of these problems are solved thanks to the customers_db_orders_snapshots that has been started in September 2019.

{% enddocs %}
{% docs customers_db_latest_trial_per_namespace %}

This model is used to build [latest_trial_per_namespaces_xf](/model.gitlab_snowflake.latest_trial_per_namespaces_xf).

It is used to build a single table summarising trials information for a specific namespace.

We start from `customers_db_orders_snapshots_base` model in order to isolate the trials. This model does the following:

* It isolates the orders that are flagged with the column `is_trial=TRUE`
* It deduplicates by taking the latest row created for a specific `gitlab_namespace_id`
* It then joins with `customers_db_customers` in order to get information about country, company_size of the user who started the trial

{% enddocs %}

{% docs customers_db_orders_with_valid_charges %}

This is an intermediate ephemeral model used in the `customers_db_charges_xf`. Each row is a different zuora rate_plan_charge with a unique `rate_plan_charge_id` key.

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

{% docs customers_db_orders_with_incomplete_charges %}

This is an intermediate ephemeral model used in the `customers_db_charges_xf`. Each row is a different zuora rate_plan_charge with a unique `rate_plan_charge_id` key.

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
