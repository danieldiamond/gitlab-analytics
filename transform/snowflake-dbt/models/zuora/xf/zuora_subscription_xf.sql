with zuora_subscription_intermediate as (

    SELECT * FROM {{ ref ('zuora_subscription_intermediate')}}

), zuora_subscription_lineage as (

    SELECT * FROM {{ ref ('zuora_subscription_lineage')}}

)

SELECT zuora_subscription_intermediate.*, 
        zuora_subscription_lineage.lineage
FROM zuora_subscription_intermediate
LEFT JOIN zuora_subscription_lineage
ON zuora_subscription_intermediate.subscription_name_slugify = 
    zuora_subscription_lineage.subscription_name_slugify