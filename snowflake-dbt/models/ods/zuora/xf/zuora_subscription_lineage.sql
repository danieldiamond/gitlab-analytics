{{config({
    "schema": "staging"
  })
}}

WITH RECURSIVE

flattening AS (

  SELECT
	{{ dbt_utils.star(from=ref('zuora_subscription_intermediate'), except=["ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY"]) }},

    IFF(array_to_string(ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY,',') IS NULL,
    subscription_name_slugify,
    subscription_name_slugify || ',' || array_to_string(ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY,','))
     							AS lineage,
    renewal.value::string 		AS ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY
  FROM {{ref('zuora_subscription_intermediate')}},
    LATERAL flatten(input => zuora_renewal_subscription_name_slugify, OUTER => TRUE) renewal

),

zuora_sub (base_slug, renewal_slug, parent_slug, lineage, children_count) AS (

  SELECT
    subscription_name_slugify              	AS base_slug,
    zuora_renewal_subscription_name_slugify	AS renewal_slug,
    subscription_name_slugify              	AS parent_slug,
    lineage 								AS lineage,
    2     									AS children_count
  FROM flattening

  UNION ALL

  SELECT
    iter.subscription_name_slugify											AS base_slug,
    iter.zuora_renewal_subscription_name_slugify							AS renewal_slug,
    anchor.parent_slug														AS parent_slug,
    anchor.lineage || ',' || iter.zuora_renewal_subscription_name_slugify 	AS lineage,
    iff(iter.zuora_renewal_subscription_name_slugify IS NULL,
		0,
		anchor.children_count + 1) 											AS children_count
  FROM zuora_sub anchor
  JOIN flattening iter
    ON anchor.renewal_slug = iter.subscription_name_slugify

),

pull_full_lineage AS (

  SELECT
	parent_slug,
	base_slug,
	renewal_slug,
	first_value(lineage)
		OVER (
		  PARTITION BY parent_slug
		  ORDER BY children_count DESC
			) 			AS lineage,
	children_count
  FROM zuora_sub

),

deduped AS (

    SELECT
      parent_slug AS subscription_name_slugify,
      lineage
    FROM pull_full_lineage
    GROUP BY 1, 2

)

SELECT *
FROM deduped
