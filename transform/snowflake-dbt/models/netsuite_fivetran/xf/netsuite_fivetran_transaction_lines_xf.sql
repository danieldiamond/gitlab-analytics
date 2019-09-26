{{
    config({
        "schema": "sensitive_analysis",
        "post-hook": "grant select on {{this}} to role reporter_sensitive"
    })
}}

WITH base AS  (

  SELECT * FROM {{ref('netsuite_transaction_lines')}}

), netsuite_fivetran_accounts AS (

  SELECT * FROM {{ref('netsuite_accounts')}}

), netsuite_fivetran_subsidiaries AS (

  SELECT * FROM {{ref('netsuite_subsidiaries')}}

)

SELECT {{ dbt_utils.star(from=ref('netsuite_transaction_lines'),
            except=['MEMO'], relation_alias='base') }},
        netsuite_accounts.account_name,
        netsuite_accounts.account_full_name,
        netsuite_accounts.account_full_description,
        netsuite_accounts.account_number,
        netsuite_accounts.account_type,
        CASE
            WHEN lower(netsuite_accounts.account_name) LIKE '%contract%'
              THEN substring(md5(netsuite_subsidiaries.subsidiary_name), 16)
            ELSE netsuite_subsidiaries.subsidiary_name END AS subsidiary_name,
        CASE
            WHEN lower(netsuite_accounts.account_name) LIKE '%contract%'
              THEN substring(md5(base.memo),16)
            ELSE base.memo END                                      AS memo
FROM base
LEFT JOIN netsuite_accounts
  ON base.account_id = netsuite_accounts.account_id
LEFT JOIN netsuite_subsidiaries
  ON base.subsidiary_id = netsuite_subsidiaries.subsidiary_id
