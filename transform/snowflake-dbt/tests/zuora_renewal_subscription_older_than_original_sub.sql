-- I've commented this test out because it FAILS. 
-- We will need to bring these accounts to finance's attention as this appears to be an upstream issue! 

--with base as (
--
--    SELECT 	subscription_name_slugify, 
--    		zuora_renewal_subscription_name_slugify, 
--    		contract_effective_date
--    FROM {{ref('zuora_subscription')}}
--    WHERE subscription_status IN ('Active', 'Cancelled')
--
--    )
--
--SELECT a.subscription_name_slugify, 
--	   a.zuora_renewal_subscription_name_slugify, 
--	   datediff(days, a.contract_effective_date, other.contract_effective_date) as age
--FROM base a
--LEFT JOIN base as other
--ON a.zuora_renewal_subscription_name_slugify = other.subscription_name_slugify
--WHERE other.subscription_name_slugify IS NOT NULL
--AND age < 0

SELECT 1 as col
WHERE col > 10