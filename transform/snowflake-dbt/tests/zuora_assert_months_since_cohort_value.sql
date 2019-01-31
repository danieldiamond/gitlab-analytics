-- Ensures that the months since the start of the cohort can never be less than 0.
-- Filters are on accounts that finance has already been alerted need fixing
-- We only use the hash when the slugs have identifying customer info

SELECT *, md5(subscription_name_slugify) as new_slug
FROM {{ ref('zuora_mrr_totals') }}
WHERE MONTHS_SINCE_ZUORA_SUBSCRIPTION_COHORT_START < 0
AND subscription_name_slugify NOT IN ('a-s00001860', 'a-s00002115', 'a-s00002227', 'a-s00002581',
	'a-s00002613', 'a-s00002613', 'a-s00002665', 'a-s00002925',
	'a-s00002964', 'a-s00003034', 'a-s00003034', 'a-s00003387',
	'a-s00003388', 'a-s00003735', 'a-s00003744', 'a-s00003970',
	'a-s00003975', 'a-s00004356', 'a-s00004452', 'a-s00004518', 'a-s00004766',
	'a-s00006360', 'a-s00006957', 'a-s00007429', 'a-s00007811', 'a-s00008366',
	'a-s00008874', 'a-s00011769', 'a-s00007582')
AND new_slug NOT IN ('c51e6ec46f7e2f2f1adc4b099ff71d86', 'f259dbbfcb59df15a38bb4f7a0f36b35', 'ae46bfc72f3d1d09f41a4e860cf6f7e7', '77ce753261f904ae25888aaa0e007956')
