{% set next_in_lineage = "||ifnull(nullif((','||nvl(b.zuora_renewal_subscription_name_slugify, '')), ','), ''))" %}

with source as (

	SELECT subscription_name,
			subscription_name_slugify,
			zuora_renewal_subscription_name_slugify as zuora_renewal_subscription_name_slugify
	FROM {{ref('zuora_subscription_intermediate')}} 

), level_1 as (

	SELECT a.subscription_name_slugify, a.zuora_renewal_subscription_name_slugify,
	       b.zuora_renewal_subscription_name_slugify as two,
	      (a.zuora_renewal_subscription_name_slugify {{ next_in_lineage }} as lineage
	FROM source a
	LEFT JOIN source as b
	ON a.zuora_renewal_subscription_name_slugify = b.subscription_name_slugify

), level_2 as (

	SELECT a.subscription_name_slugify, a.zuora_renewal_subscription_name_slugify,
	       b.zuora_renewal_subscription_name_slugify as two,
	      (a.lineage {{ next_in_lineage }} as lineage
	FROM level_1 as a
	LEFT JOIN source as b
	ON a.two = b.subscription_name_slugify

), level_3 as (

	SELECT a.subscription_name_slugify, a.zuora_renewal_subscription_name_slugify,
	       b.zuora_renewal_subscription_name_slugify as two,
	      (a.lineage {{ next_in_lineage }} as lineage
	FROM level_2 as a
	LEFT JOIN source as b
	ON a.two = b.subscription_name_slugify

), level_4 as (

	SELECT a.subscription_name_slugify, a.zuora_renewal_subscription_name_slugify,
	       b.zuora_renewal_subscription_name_slugify as two,
	      (a.lineage {{ next_in_lineage }} as lineage
	FROM level_3 as a
	LEFT JOIN source as b
	ON a.two = b.subscription_name_slugify

), level_5 as (

	SELECT a.subscription_name_slugify, a.zuora_renewal_subscription_name_slugify,
	       b.zuora_renewal_subscription_name_slugify as two,
	      (a.lineage {{ next_in_lineage }} as lineage
	FROM level_4 as a
	LEFT JOIN source as b
	ON a.two = b.subscription_name_slugify

), level_6 as (

	SELECT a.subscription_name_slugify,  -- note that the final depth here only produces two columns!
			(a.lineage {{ next_in_lineage }} as lineage
	FROM level_5 as a
	LEFT JOIN source as b
	ON a.two = b.subscription_name_slugify

), deduped as (

	SELECT *
	FROM level_6
	GROUP BY 1, 2

)

SELECT *
FROM deduped