{{ config({
    "schema": "staging"
    })
}}

WITH netsuite_transaction_lines AS  (

  SELECT * FROM {{ref('netsuite_transaction_lines')}}

), netsuite_accounts AS (

  SELECT * FROM {{ref('netsuite_accounts')}}

), netsuite_subsidiaries AS (

  SELECT * FROM {{ref('netsuite_subsidiaries')}}

)

SELECT netsuite_transaction_lines.transaction_lines_unique_id,
       netsuite_transaction_lines.transaction_id,
       netsuite_transaction_lines.transaction_line_id,
       netsuite_transaction_lines.account_id,
       netsuite_transaction_lines.department_id,
       netsuite_transaction_lines.subsidiary_id,
       netsuite_transaction_lines.amount,
       netsuite_transaction_lines.gross_amount,
       netsuite_accounts.account_name,
       netsuite_accounts.account_full_name,
       netsuite_accounts.account_full_description,
       netsuite_accounts.account_number,
       netsuite_accounts.account_type,
       CASE WHEN lower(netsuite_accounts.account_name) LIKE '%contract%'
         THEN substring(md5(netsuite_subsidiaries.subsidiary_name), 16)
         ELSE netsuite_subsidiaries.subsidiary_name
       END                                      AS subsidiary_name,
       CASE WHEN lower(netsuite_accounts.account_name) LIKE '%contract%'
         THEN substring(md5(netsuite_transaction_lines.memo),16)
         ELSE netsuite_transaction_lines.memo
       END                                      AS memo
FROM netsuite_transaction_lines
LEFT JOIN netsuite_accounts
  ON netsuite_transaction_lines.account_id = netsuite_accounts.account_id
LEFT JOIN netsuite_subsidiaries
  ON netsuite_transaction_lines.subsidiary_id = netsuite_subsidiaries.subsidiary_id
