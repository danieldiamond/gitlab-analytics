{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}


WITH entries AS (
    SELECT *
    FROM {{ref('netsuite_stitch_all_entries')}}
),

grouped AS (
  SELECT
    account_name,
    ultimate_account_code,
    transaction_id,
    transaction_type,
    CASE
      WHEN transaction_type = 'JournalEntry'
        THEN ' '
      ELSE entity
    END AS entity_name,
    MAX(transaction_date)                    AS transaction_date,
    MAX(period_date)                         AS period_date,
    SUM(debit_amount)                        AS debit,
    SUM(credit_amount)                       AS credit
  FROM
    entries
  GROUP BY
  1,2,3,4,5
)

SELECT
  transaction_type,
  transaction_date,
  period_date,
  account_name,
  ultimate_account_code,
  transaction_id,
  entity_name,
  CASE
    WHEN debit != 0 AND credit != 0
    THEN IFF(debit - credit > 0, debit - credit, 0)
    ELSE debit
  END AS debit,
  CASE
    WHEN debit != 0 AND credit != 0
    THEN IFF(debit - credit < 0, ABS(debit - credit), 0)
    ELSE credit
  END AS credit

FROM
   grouped