{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH filter AS (

    SELECT zuora_subscription_id,
           original_mrr,
           retention_month,
           rank() over(partition by zuora_subscription_id order by retention_month asc) AS rank
    FROM {{ ref('retention_reasons_for_retention') }}

)

SELECT *
FROM filter