{{
    config({
        "schema": "sensitive_analysis",
        "post-hook": "grant select on {{this}} to role reporter_sensitive"
    })
}}

WITH base AS  (

  SELECT * FROM {{ref('netsuite_fivetran_transaction_lines')}}

), netsuite_fivetran_accounts AS (

  SELECT * FROM {{ref('netsuite_fivetran_accounts')}}

), netsuite_fivetran_subsidiaries AS (

  SELECT * FROM {{ref('netsuite_fivetran_subsidiaries')}}

)

SELECT {{ dbt_utils.star(from=ref('netsuite_fivetran_transaction_lines'),
            except=['MEMO'], relation_alias='base') }},
        netsuite_fivetran_accounts.account_name,
        netsuite_fivetran_accounts.account_full_name,
        netsuite_fivetran_accounts.account_full_description,
        netsuite_fivetran_accounts.account_number,
        netsuite_fivetran_accounts.account_type,
        CASE
            WHEN lower(netsuite_fivetran_accounts.account_name) LIKE '%contract%'
              THEN substring(md5(netsuite_fivetran_subsidiaries.subsidiary_name), 16)
            ELSE netsuite_fivetran_subsidiaries.subsidiary_name END AS subsidiary_name,
        CASE
            WHEN lower(netsuite_fivetran_accounts.account_name) LIKE '%contract%'
              THEN substring(md5(base.memo),16)
            ELSE base.memo END                                      AS memo
FROM base
LEFT JOIN netsuite_fivetran_accounts
  ON base.account_id = netsuite_fivetran_accounts.account_id
LEFT JOIN netsuite_fivetran_subsidiaries
  ON base.subsidiary_id = netsuite_fivetran_subsidiaries.subsidiary_id
