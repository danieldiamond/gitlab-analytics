WITH transaction_lines AS  (

  SELECT *
  FROM {{ref('netsuite_transaction_lines')}}

), transactions AS  (

  SELECT *
  FROM {{ref('netsuite_transactions')}}

), accounts AS (

  SELECT *
  FROM {{ref('netsuite_accounts')}}

), subsidiaries AS (

  SELECT *
  FROM {{ref('netsuite_subsidiaries')}}

), entity AS (

  SELECT *
  FROM {{ref('netsuite_entity')}}

)

SELECT
  tl.transaction_lines_unique_id,
  tl.transaction_id,
  tl.transaction_line_id,
  tl.account_id,
  tl.class_id,
  tl.department_id,
  tl.subsidiary_id,
  tl.receipt_url,
  tl.amount,
  tl.gross_amount,
  a.account_name,
  a.account_full_name,
  a.account_full_description,
  a.account_number,
  a.account_type,
  CASE
    WHEN LOWER(a.account_name) LIKE '%contract%'
      THEN SUBSTRING(md5(s.subsidiary_name), 16)
    ELSE s.subsidiary_name
  END                                      AS subsidiary_name,
  CASE
    WHEN LOWER(a.account_name) LIKE '%contract%'
      THEN SUBSTRING(md5(tl.memo),16)
    ELSE tl.memo
  END                                      AS memo,
  CASE
    WHEN LOWER(a.account_name) LIKE '%contract%'
      THEN SUBSTRING(md5(e.entity_name),16)
    WHEN t.entity_id IS NOT NULL
      THEN e2.entity_name
    ELSE e.entity_name
  END                                      AS entity_name
FROM transaction_lines tl
LEFT JOIN transactions t
  ON tl.transaction_id = t.transaction_id
LEFT JOIN entity e
  ON tl.company_id = e.entity_id
LEFT JOIN entity e2
  ON t.entity_id = e2.entity_id
LEFT JOIN accounts a
  ON tl.account_id = a.account_id
LEFT JOIN subsidiaries s
  ON tl.subsidiary_id = s.subsidiary_id
