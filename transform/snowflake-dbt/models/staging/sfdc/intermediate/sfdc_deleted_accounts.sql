with source as (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE account_id IS NOT NULL
      AND is_deleted = TRUE
)

SELECT
  a.account_id                                     AS sfdc_account_id,
  COALESCE(a.master_record_id, b.master_record_id) AS sfdc_master_record_id
FROM source a
LEFT JOIN source b
  ON a.master_record_id = b.account_id

