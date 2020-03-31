with source as (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}

)

SELECT
    account_id AS sfdc_account_id,
    master_record_id AS sfdc_master_record_id
FROM source
WHERE account_id IS NOT NULL
  AND is_deleted = TRUE
