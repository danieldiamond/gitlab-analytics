with source as (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}

)

SELECT sfdc_account_id,
    sfdc_master_record_id
FROM source
WHERE sfdc_account_id IS NOT NULL
    AND is_deleted = TRUE
