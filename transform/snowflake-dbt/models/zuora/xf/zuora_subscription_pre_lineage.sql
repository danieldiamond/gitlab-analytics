SELECT
    {{ dbt_utils.star(from=ref('zuora_subscription_intermediate'), except=["ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY"]) }},

    IFF(array_to_string(ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY,',') IS NULL,
        subscription_name_slugify,
        subscription_name_slugify || ',' || array_to_string(ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY,','))
     as lineage,
    renewal.value::string as ZUORA_RENEWAL_SUBSCRIPTION_NAME_SLUGIFY
FROM {{ref('zuora_subscription_intermediate')}},
    LATERAL flatten(input => zuora_renewal_subscription_name_slugify, OUTER => TRUE) renewal