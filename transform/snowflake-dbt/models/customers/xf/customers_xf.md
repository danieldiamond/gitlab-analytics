{% docs customers_db_charges_xf %}

This model creates a clean table that one can easily join on `zuora_base_mrr` tables to have some financial information. It deduplicates, transforms and joins on `zuora_subscription_xf`, `zuora_rate_plan` and `zuora_rate_plan_charge` in order to create a table at the granularity of one charge per row.

This model first unions the 2 ephemeral models `customers_db_charges_with_valid_charges` and `customers_db_charges_with_incomplete_charges` which provides a clean list of all orders that have been created in the subscription portal and that can be linked to Zuora subscriptions and charges.

The union of these 2 tables is then easily joined on `zuora_base_mrr` to add financial information such as `mrr`, `tcv`, `quantity_ordered`... 

{% enddocs %}

{% docs customers_db_trials %}

This model collects all trials started from the subscription portal. For this we use the `customers_db_orders_snapshots_base` model in order to isolate them. This model does the following thing:

* It isolates the orders that are flagged with the column `is_trial=TRUE`
* It deduplicates by taking the first row created
* It joins with customers, users and namespaces. 

Finally, this model identifies if a trial has been converted or not. To achieve that, we join the selected trials to the order_snapshots selecting only orders converted to subscription after the trial starting date  (look at example below). We exclude ci_minutes orders from the order_snapshots.   

There is one trick here to identify which subscriptions are actually valid and not refunded. In order to do so, we join on `zuora_rate_plan` and `zuora_rate_plan_charge` in order to filter out subscriptions that have (mrr <= 0 and tcv <=0). One of the case we filter out are those subscriptions that are cancelled instantly or fully refunded after a certain period.

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

{% docs customers_db_trials %}

This model collects for a specific namespace the latest trials information. 


## Context

To understand the context, the following information is important:
* before 2019-09-16, a namespace can subscribe several times to a trial. That was a bug corrected by the fulfillment team in September 2019. 
* all snapshots tables have also been created in September 2019. Before that we don't have historical information
* customers_db ETL was before October 2019 unstable. We improved the logic at the end of October by changing from incremental model to a daily full "drop and create" to the raw database.

## Process

Trial Information is collected in 2 tables (one in the subscription portal database - customer_db, the other in the .com database - main app). These 2 tables have incomplete information. We join them together to get a more consistent and complete picture of trials started

For the gitlab_dotcom database, information is stored in `gitlab_dotcom_gitlab_subscriptions` table. As described [here](LINK), rows can be deleted in this table, so we use the `gitlab_dotcom_gitlab_subscriptions_snapshot` for higher reporting accuracy.  In this model, we do the following operations:
* We isolate trials by looking at a specific column `gitlab_subscription_trial_ends_on` which is filled only when a specific subscription was a trial before.
* We then group by the namespace_id in order to only select the latest trials started for a specific namespace.
* One weird behaviour of this table is the way it deals with expired orders. It is explained [here](LINK) That means that the `start_date` is NOT a reliable information for us in order to know the trial start date. We therefore use the `gitlab_subscription_trial_ends_on` column in order to estimate when the trial has been started (30 days before the end of the trials in most cases)

For the customers database, it is explained in the models `customers_db_latest_trials_per_namespace`

We then join the 2 CTEs created on `gitlab_namespace_id`.

Finally, this model identifies if a trial has been converted or not. To achieve that, we join the selected trials to the order_snapshots selecting only orders converted to subscription after the trial starting date  (look at example below). We exclude ci_minutes orders from the order_snapshots.   

There is one trick here to identify which subscriptions are actually valid and not refunded. In order to do so, we join on `zuora_rate_plan` and `zuora_rate_plan_charge` in order to filter out subscriptions that have (mrr <= 0 and tcv <=0). One of the case we filter out are those subscriptions that are cancelled instantly or fully refunded after a certain period.


## Table Examples

### customers_db_orders

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
