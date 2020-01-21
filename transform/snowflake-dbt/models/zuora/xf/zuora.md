{% docs zuora_base_mrr %}

This model generates an entry for each unique charge associated with a subscription. The specific information about what is happening with a subscription is stored with the rate plan charge. That RPC is part of a Rate Plan, which is then linked to a subscription. There can be multiple rate plans and charges per subscription. The effective start and end dates define the time period that a charge is effective.

We only care about charges that have both Monthly Recurring Revenue (MRR) > $0 and Total Contract Value (TCV) > $0.

For the purposes of applying MRR to months, we only care about what rate plan charge was in effect on the very last day of the month. The effective_end_date calculation is taken as the previous month for the following reason: technically, on Zuora's side, the effective end date stored in the database is the day _after_ the subscription ended. (More info here https://community.zuora.com/t5/Subscriptions/How-to-get-ALL-the-products-per-active-subscription/td-p/2224) Another way to think about it is that the effective end date is the first day of the renewal. By subtracting the month, we're guaranteed to get the correct month for an end date. If in the DB it ends 7/31, then in reality that is the day before (7/30) and is therefore not in effect for the month of July (because it has to be in effect on the last day to be in force for that month). If the end date is 8/1, then it is in effect for the month of July and we're making the proper calculation by subtracting 1 month.

To reiterate, if there is a rate plan charge that starts 2017-03-15Å and goes to 2017-06-10, then that charge would be counted for the months of March, April, and May. June is not counted because a different charge would be in effect on the last day of the month.

Another example: if the effective start and end dates of the charge occurred from 2018-08-02 to 2018-08-15, that would be completely dropped. If it occurred from 2018-08-16 to 2018-08-31, that would also be dropped because the charge was not in force on the last day of the month. If it occurred from 2018-08-30 to 2018-09-02, then that would be in force for the month of August.

The final WHERE filter validates that only charges that were in force at the end of the month are selected. Using the 2018-08-16 to 2018-08-31 example, the effective start and end months would be 2018-08-01 and 2018-07-01 which would result in a datediff value of -1 and would be filtered out.

{% enddocs %}


{% docs zuora_base_mrr_amortized %}

This table amortizes the monthly charges over the time span that the rate plan charge was in effect. A rate plan charge is only in effect if it was in effect for the last day of the month. 

{% enddocs %}

{% docs zuora_base_invoice_details %}

This table defines each invoice charge. Charges come from invoice items which are part of Invoices. Invoices must be "Posted".

The CTE `sub_months` pulls the unique combination of account number, cohort month, and the subscription identifiers so that the linkage to the charge can be made. This works as-is because of the upfront work on the subscription modeling to de-duplicate the data and account for renewals. Every subscription name is by default unique, so the slug should be as well.

The `charges` CTE is specifically crafted according to the data needs. The first join is INNER because we only want the charges that return a valid Invoice (e.g. we don't want a null invoice_id). The remainder are LEFT JOINS because we want to maintain all of the valid invoice item charges.

{% enddocs %}


{% docs zuora_base_ci_minutes %}

This table defines each CI minutes charge. The appropriate charge name is "1,000 CI Minutes".

CI minutes are not currently assigned to MRR.

{% enddocs %}


{% docs zuora_base_trueups %}

This table defines each trueup charge. The appropriate charge name for a trueup is either "Trueup" or "Trueup Credit" (which has a negative value).

The final select statement then allocates the trueup according to what's listed in the [operating metrics](https://about.gitlab.com/handbook/finance/operating-metrics/#annual-recurring-revenue-arr) in our handbook  Simply put, the charge_amount for the trueup is divided by 12 and applied to the MRR for the month the trueup occurred (see also [Trueup Pricing](https://about.gitlab.com/handbook/product/pricing/#true-up-pricing)).


{% enddocs %}


{% docs zuora_mrr_totals %}

This model unions the base charges and the trueup charges together. For each month we calculate the number of months between the start of the cohort and the current month. This enables the data to be easily filtered in the BI tool so you can look across multiple cohorts and limit the months into the future to the same number. This value should never be less than 0.

We then aggregate the data into one row per Month for each unique (subscription || product || unit of measurement) combination. At this time, this is the most granular value (even more than subscription) and will be the foundation for calculation retention by product.

{% enddocs %}


{% docs zuora_subscription_intermediate %}

The `zuora_subs` CTE de-duplicates Zuora subscriptions. Zuora keeps track of different versions of a subscription via the field "version". However, it's possible for there to be multiple version of a single Zuora version. The data with account_id = '2c92a0fc55a0dc530155c01a026806bd' in the base zuora_subscription table exemplifies this. There are multiple rows with a version of 4. The CTE adds a row number based on the updated_date where a value of 1 means it's the newest version of that version. It also filters subscriptions down to those that have either "Active" or "Cancelled" statuses since those are the only ones that we care about.

The `renewal_subs` CTE creates a lookup table for renewal subscriptions, their parent, and the earliest contract start date. The `contract_effective_date` field was found to be the best identifier for a subscriptions cohort, hence why we're finding the earliest relevant one here. The renewal_row is generated because there are instances where multiple subscriptions point to the same renewal. We generally will want the oldest one for info like cohort date.

The final select statement creates a new field specifically for counting subscriptions and generates appropriate cohort dates. Because we want to count renewal subscriptions as part of their parent, we have the slug for counting so that we don't artificially inflate numbers. It also pickes the most recent version of a subscription.

The subscription_end_month calculation is taken as the previous month for a few reasons. Technically, on Zuora's side, the effective end date stored in the database the day _after_ the subscription ended. (More info here https://community.zuora.com/t5/Subscriptions/How-to-get-ALL-the-products-per-active-subscription/td-p/2224) By subtracting the month, we're guaranteed to get the correct month for an end date. If in the DB it ends 7/31, then in reality that is the day before and is therefore not in effect for the month of July (because it has to be in effect on the last day to be in force for that month). If the end date is 8/1, then it is in effect for the month of July and we're making the proper calculation.

{% enddocs %}

{% docs zuora_subscription_lineage %}

Connects a subscription to all of the subscriptions in its lineage. To understand more about a subscription's relationship to others, please see [the handbook under Zuora Subscription Data Management](https://about.gitlab.com/handbook/finance/accounting/)

The `flattening` CTE flattens the intermediate model based on the array in the renewal slug field set in the base subscription model. Lineage is initially set here as the values in the parent slug and any renewal slugs. The OUTER => TRUE setting is like doing an outer join and will return rows even if the renewal slug is null.  

The recursive CTE function generate the full lineage. The anchor query pulls from the flattening CTE and sets up the initial lineage. If there is a renewal subscription then it will continue to the next part of the CTE, but if there are no renewals then the recursive clause will return no additional results.

The recursive clause joins the renewal slug from the anchor clause to the subscription slug of the next iteration of the recursive clause. We're keeping track of the parent slug as the "root" for the initial recursion (this is the "ultimate parent" of the lineage). Within the recursive clause we're checking if there are any further renewals before setting the child count.

The next CTE takes the full union of the results and finds the longest lineage for every parent slug based on the children_count. This CTE is overexpressive and could most likely be simplified with the deduplication CTE. The final dedupe CTE returns a single value for every root and it's full downstream lineage.

{% enddocs %}

{% docs zuora_subscription_parentage_start %}
This is the first part of a two-part model. (It is in two parts because of memory constraints.)

The `flattened` CTE takes the data from lineage, which starts in the following state:


|SUBSCRIPTION_NAME_SLUGIFY|LINEAGE|
|:-:|:-:|
|a-s00011816|a-s00011817,a-s00011818|
|a-s00011817|a-s00011818|
|a-s00003063|a-s00011816,a-s00011817,a-s00011818|


This flattens them to be be in one-per row. Rxample:

|SUBSCRIPTION_NAME_SLUGIFY|SUBSCRIPTIONS_IN_LINEAGE|CHILD_INDEX|
|:-:|:-:|:-:|
|a-s00011817|a-s00011818|0|
|a-s00011816|a-s00011817|0|
|a-s00011816|a-s00011818|1|
|a-s00003063|a-s00011816|0|
|a-s00003063|a-s00011817|1|

Then we identify the version of the `subscriptions_in_lineage` with the max depth (in the `find_max_depth` CTE) and join it to the `flattened` CTE in the `with_parents` CTE. This allows us to identify the ultimate parent subscription in any given subscription.

For this series of subscriptions, the transformation result is:

|ULTIMATE_PARENT_SUB|CHILD_SUB|DEPTH|
|:-:|:-:|:-:|
|a-s00003063|a-s00011816|0|
|a-s00003063|a-s00011817|1|
|a-s00003063|a-s00011818|2|

Of note here is that parent accounts _only_ appear in the parents column. `a-s00003063` does not appear linked to itself. (We correct for this in `subscriptions_xf` when introducing the `subscription_slug_for_counting` value and coalescing it with the slug.)

In the final CTE `finalish`, we join to intermediate to retreive the cohort dates before joining to `subscription_intermediate` in `subscription_xf`.

The end result of those same subscriptions:

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003063|a-s00011816|2014-08-01|2014-07-01|2014-01-01|
|a-s00003063|a-s00011817|2014-08-01|2014-07-01|2014-01-01|
|a-s00003063|a-s00011818|2014-08-01|2014-07-01|2014-01-01|

This transformation process does not handle the consolidation of subscriptions, though, which is what `zuora_subscription_parentage_finish` picks up.

{% enddocs %}

{% docs zuora_subscription_parentage_finish %}

This is the second part of a two-part model. (It is in two parts because of memory constraints.) For the first part, please checkout the docs for zuora_subscription_parentage_start.

Some accounts are not a direct renewal, they are the consolidation of many subscriptions into one. While the lineage model is build to accomodate these well, simply flattening the model produces one parent for many children accounts, for example:

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003114|a-s00005209|2016-01-01|2016-01-01|2016-01-01|
|a-s00003873|a-s00005209|2017-01-01|2017-01-01|2017-01-01|

Since the whole point of ultimate parent is to understand cohorts, this poses a problem (not just for fan outs when joining) because it is inaccurate.

The `new_base` CTE identifies all affected subscriptions, while `consolidated_parents` and `deduped_parents` find the oldest version of the subscription.

This produces

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003114|a-s00005209|2016-01-01|2016-01-01|2016-01-01|

but drops the subscriptions that are not the ultimate parent but had not previously been identified as children, in this case `a-s00003873`.

The first part of the `unioned` CTE isolates these subscriptions, naming them children of the newly-minted ultimate parent subscription (really just the oldest in a collection of related subscriptions), producing

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003114|a-s00003873|2016-01-01|2016-01-01|2016-01-01|
|a-s00003114|a-s00003873|2016-01-01|2016-01-01|2016-01-01|


It unions this to the results of `deduped_consolidations` and all original base table where the subscriptions were not affected by consolidations. Finally we deduplicate one more time.  

The final result:

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003114|a-s00009998|2016-01-01|2016-01-01|2016-01-01|
|a-s00003114|a-s00003873|2016-01-01|2016-01-01|2016-01-01|
|a-s00003114|a-s00005209|2016-01-01|2016-01-01|2016-01-01|


{% enddocs %}

{% docs zuora_subscription_periods %}

This table is the xf table for valid Zuora subscription periods. A subscription period is an interval (bounded by `term_start_date` and `term_end_date`) during which a specific version was valid.

In a more explicit way, this shows, when looking at a specific date (past or  future), what was/will be the active subscription version at this time.

From this model, we can surface renewal rates by product category. We can also start estimating IACV, Renewal ACV and other metrics for the Growth team. A `subscription_period` is considered as renewed if a newer valid subscription period has been created or if a `zuora_renewal_subscription_name_slugify` has been linked to this version (more documentation about [the process here](LINK)) (in this model, the `is_renewed` flag will be turned to `TRUE`).

#### Some context about subscription versions

[Zuora Subscription Version Documentation](LINK)

The way GitLab works with version is quite confusing. For subscription with `auto_renew` turned on, a new subscription version is automatically created when the subscription expires (without processing credit card payment). If the payment fails, a new version (similar to the previous one) is created, auto_renew is turned to off and status stays as `active`.

For all other  subscriptions, any change in the subscription T&Cs and settings (product, seats, end date, price...) will create a new version of the subscription. That means that some subscriptions have up to 20 versions when they actually had only 2 renewals (`subscription_id = ''` for a sales generated one and `subscription_id = ''`  for one that has been created on the customers portal are 2 good examples).

#### Technical explanations

The model wants to identify which versions have been valid. In order to do so, the model is built recursively starting from the latest subscription version (version column is an incremental counter). This one has always `Cancelled` or `Active` status. We assume that this one is currently valid and shows the latest state of the subscription.

To check if the previous version have been valid at some point, we will compare the `term_start_date` between the freshest and the one before. If the `term_start_date` is in the future or on the same day as the latest version, we assume that this version has been never properly validated and got rolled back. For a specific version, we look at all newer versions (with higher version number), and check the minimum `term_start_date` in this subset of versions. If the `term_start_date` of the version checked, is greater or equal to the minimum of the newer ones, we assume that this one has never been valid, and we filter it out.  


{% enddocs %}
