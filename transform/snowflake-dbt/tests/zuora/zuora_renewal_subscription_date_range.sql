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
     AND account_id NOT IN (
       '2c92a0fe59b55c400159d7c1f2550f81', --https://gitlab.com/gitlab-data/analytics/issues/2966
       '2c92a0fe5f912d8e015f98f5b02411b5', --https://gitlab.com/gitlab-data/analytics/issues/2966
       '2c92a0076b6403ed016b65a237774f34'  --https://gitlab.com/gitlab-data/analytics/issues/2966
     )


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
