WITH RECURSIVE zuora_sub (base_slug, renewal_slug, parent_slug, lineage, children_count) AS (

	SELECT
    subscription_name_slugify                                                        AS base_slug,
    zuora_renewal_subscription_name_slugify                                          AS renewal_slug,
    subscription_name_slugify                                                        AS parent_slug,
    IFF(zuora_renewal_subscription_name_slugify IS NULL,
        subscription_name_slugify,
        subscription_name_slugify || ',' || zuora_renewal_subscription_name_slugify) AS lineage,
    2                                                                                AS children_count
  FROM {{ref('zuora_subscription_intermediate')}}

  UNION ALL

  SELECT
    iter.subscription_name_slugify                                                       AS base_slug,
    iter.zuora_renewal_subscription_name_slugify                                         AS renewal_slug,
    anchor.parent_slug                                                                     AS parent_slug,
    anchor.lineage || ',' || iter.zuora_renewal_subscription_name_slugify                   AS lineage,
    iff(iter.zuora_renewal_subscription_name_slugify IS NULL, 0, anchor.children_count + 1) AS children_count
  FROM zuora_sub anchor
    JOIN {{ref('zuora_subscription_intermediate')}} iter
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