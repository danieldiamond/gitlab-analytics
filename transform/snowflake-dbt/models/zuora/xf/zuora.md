{% docs zuora_base_mrr %}

This model generates an entry for each unique charge associated with a subscription. The specific information about what is happening with a subscription is stored with the rate plan charge. That RPC is part of a Rate Plan, which is then linked to a subscription. There can be multiple rate plans and charges per subscription. The effective start and end dates define the time period that a charge is effective.

We only care about charges that have both Monthly Recurring Revenue (MRR) > $0 and Total Contract Value (TCV) > $0.

The other WHERE filter is intended to capture only the rate plan charge that is valid at the end of the month. For example, if there is a rate plan charge that starts March 15, 2017 and goes to June 10, 2017, then that charge would be counted for the months of March, April, and May. June is not counted because a different charge would be in effect on the last day of the month. The filter specifically does not include charges that have a start and end date in the same month as long as neither of those dates happen ON the last day of the month.

For example, if a charge occurred from August 2 to August 15, that would be dropped. If it occurred from August 16 to August 31, that would be kept because the charge was in force on the last day of the month. If it occurred from August 30 to September 2, then that would also be in force for the month of August. 

For the purposes of counting, we care about what was in effect on the very last day of the month. The effect_end_Date calculation is taken as the previous month for a few reasons. Technically, on Zuora's side, the effective end date stored in the database the day _after_ the subscription ended. (More info here https://community.zuora.com/t5/Subscriptions/How-to-get-ALL-the-products-per-active-subscription/td-p/2224) By subtracting the month, we're guaranteed to get the correct month for an end date. If in the DB it ends 7/31, then in reality that is the day before and is therefor not in effect for the month of July (because it has to be in effect on the last day to be in force for that month). If the end date is 8/1, then it is in effect for the month of July and we're making the proper calculation. 


{% enddocs %}


{% docs zuora_base_mrr_amortized %}

This table amortizes the monthly charges over the time span that the rate plan charge was in effect. A rate plan charge is only in effect if it was in effect for the last day of the month. 


{% enddocs %}


{% docs zuora_base_trueups %}

This table defines each trueup charge. Trueups come from invoice items which are part of Invoices. The appropriate charge name for a trueup is either "Trueup" or "Trueup Credit" (which has a negative value). Invoices must be "Posted".

The CTE `sub_months` pulls the unique combination of account number, cohort month, and the subscription identifiers so that the linkage to the Trueup can be made. This works as-is because of the upfront work on the subscription modeling to de-duplicate the data and account for renewals. Every subscription name is by default unique, so the slug should be as well. 

The `trueups` CTE is specifically crafted according to the data needs. The first join is INNER because we only want the charges that return a valid Invoice (e.g. we don't want a null invoice_number). The remained are LEFT JOINS because we want to maintain all of the valid invoice item charges.

The final select statement then allocates the trueup according to what's listed in the [operating metrics](https://about.gitlab.com/handbook/finance/operating-metrics/#annual-recurring-revenue-arr) in our handbook  Simply put, the charge_amount for the trueup is divided by 12 and applied to the MRR for the month the trueup occurred (see also [Trueup Pricing](https://about.gitlab.com/handbook/product/pricing/#true-up-pricing)). 


{% enddocs %}


{% docs zuora_mrr_totals %}

This model unions the base charges and the trueup charges together. For each month we calculate the number of months between the start of the cohort and the current month. This enables the data to be easily filtered in Looker so you can look across multiple cohorts and limit the months into the future to the same number. This value should never be less than 0.

{% enddocs %}


{% docs zuora_subscription %}

This table is the base table for Zuora subscriptions.

It removes all subscriptions that are marked as "Exclude from Analysis". It also creates a slug of the subscription name and the renewal subscription name (if it exists) using the user-defined function (UDF) "zuora_slugify".

There is a test specifically to validate this.

#### Zuora Slugify UDF
This function is used to simplify Zuora subscriptions names. Subscription names, by default, are unique. However, they can have basically any character they want in them, including arbitrary amounts of whitespace. The original idea was to truly slugify the name but that wasn't unique enough across subscriptions. We did have an issue where links to renewal subscriptions were not being copied correctly. Creating a slug that simply replaces most characters with hyphens maintained uniqueness but made it easier to reason about how the data was incorrectly formatted. It in the end it may not be wholly necessary, but from a programming perspective it is a bit easier to reason about.

{% enddocs %}



{% docs zuora_subscription_intermediate %}

This table is the transformed table for Zuora subscriptions.

The `zuora_subs` CTE de-duplicates Zuora subscriptions. Zuora keeps track of different versions of a subscription via the field "version". However, it's possible for there to be multiple version of a single Zuora version. The data with account_id = '2c92a0fc55a0dc530155c01a026806bd' in the base zuora_subscription table exemplifies this. There are multiple rows with a version of 4. The CTE adds a row number based on the updated_date where a value of 1 means it's the newest version of that version. It also filters subscriptions down to those that have either "Active" or "Cancelled" statuses since those are the only ones that we care about.

The `renewal_subs` CTE creates a lookup table for renewal subscriptions, their parent, and the earliest contract start date. The `contract_effective_date` field was found to be the best identifier for a subscriptions cohort, hence why we're finding the earliest relevant one here. The renewal_row is generated because there are instances where multiple subscriptions point to the same renewal. We generally will want the oldest one for info like cohort date.

The final select statement creates a new field specifically for counting subscriptions and generates appropriate cohort dates. Because we want to count renewal subscriptions as part of their parent, we have the slug for counting so that we don't artificially inflate numbers. It also pickes the most recent version of a subscription.

The subscription_end_month calculation is taken as the previous month for a few reasons. Technically, on Zuora's side, the effective end date stored in the database the day _after_ the subscription ended. (More info here https://community.zuora.com/t5/Subscriptions/How-to-get-ALL-the-products-per-active-subscription/td-p/2224) By subtracting the month, we're guaranteed to get the correct month for an end date. If in the DB it ends 7/31, then in reality that is the day before and is therefore not in effect for the month of July (because it has to be in effect on the last day to be in force for that month). If the end date is 8/1, then it is in effect for the month of July and we're making the proper calculation. 

{% enddocs %}