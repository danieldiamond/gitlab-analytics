{{
    config(
        materialized='incremental',
        unique_key='account_id'
    )
}}
WITH account_records AS (

  SELECT *
  FROM {{ ref('zuora_account_source') }}
  WHERE account_id in (
        '2c92a0fe5654120701567a1e42f22ed3',
        '2c92a0ff5e50a5f6015e76d832f80264',
        '2c92a00c6f31ee5a016f47f78d236419'
  )
)


SELECT *
FROM account_records
WHERE account_id NOT IN (SELECT DISTINCT ACCOUNT_ID FROM {{this}})
