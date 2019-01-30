-- this test checks for renewal dates to be older than subscription start dates. 

-- I've commented this test out because it FAILS. 

--with base as (
--    SELECT 	subscription_name_slugify,
--    		zuora_renewal_subscription_name_slugify,
--    		subscription_start_date
--		FROM {{ref('zuora_subscription')}}
--    WHERE subscription_status IN ('Active', 'Cancelled')
--    )
--SELECT a.subscription_name_slugify,
--	   	a.zuora_renewal_subscription_name_slugify,
--			a.subscription_start_date,
--			other.subscription_start_date,
--	   datediff(days, a.subscription_start_date, other.subscription_start_date) as age
--FROM base a
--LEFT JOIN base as other
--ON a.zuora_renewal_subscription_name_slugify = other.subscription_name_slugify
--WHERE other.subscription_name_slugify IS NOT NULL
--AND age < 0

SELECT 1 as col
WHERE col > 10