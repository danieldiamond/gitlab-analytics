-- this test checks for renewal dates to be older than subscription start dates.

WITH base AS (
   
   SELECT 	
        subscription_name_slugify,
   		renewal.value::string AS zuora_renewal_subscription_name_slugify,
   		subscription_start_date,
   		subscription_end_date
		FROM {{ref('zuora_subscription')}},
			 LATERAL flatten(input => zuora_renewal_subscription_name_slugify, OUTER => TRUE) renewal
   WHERE subscription_status IN ('Active', 'Cancelled')

   )

SELECT 	a.subscription_name_slugify,
	   	a.zuora_renewal_subscription_name_slugify,
		a.subscription_start_date                                       as original_sub_start_date,
   	    a.subscription_end_date                                         as original_sub_end_date,
		other.subscription_start_date                                   as renewal_sub_start_date,
	   	datediff(days, original_sub_start_date, renewal_sub_start_date) as age_start_dates,
	   	datediff(days, original_sub_end_date, renewal_sub_start_date)   as age_end_date_to_start_date
FROM base a
LEFT JOIN base AS other
  ON a.zuora_renewal_subscription_name_slugify = other.subscription_name_slugify
WHERE other.subscription_name_slugify IS NOT NULL
  AND (age_start_dates < 0 OR age_end_date_to_start_date > 366)
