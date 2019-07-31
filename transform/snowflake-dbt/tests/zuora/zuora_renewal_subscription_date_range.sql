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
  AND md5(a.subscription_name_slugify) NOT IN (
    '1ee38a8d0e138791fa4ec8a513b0ec3c',
    'c2072fed9c224250c477aa7549db9a4e',
    '38d64b0cea3899ccc0ecf17f7df95589',
    'bc80a9252d7e6fe358e8ae968a7a6ffa',
    '56d2be9ea12333a5a6608690610e3496',
    '95d585031baaee19fbff0ca9dd30c583',
    '975abe68ef24aa84594ca779eb64afe5',
    '639b118ab364d61919c3acd443c2dae8',
    '47d5cf76786f8eacd6ff91ac02c3a147',
    'f7222040241122188574d5befbae6497',
    '0d8e569929631bb407b6ca05e8c93b69',
    'a546894e31d4d3ce8df896922c89c8e9',
    '140a193a7bb421f6df07c0967638c6dc'
    )
