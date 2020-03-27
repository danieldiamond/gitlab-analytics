with source as (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}

)

SELECT *
FROM source
WHERE sfdc_account_id IS NOT NULL
    AND is_deleted = FALSE
