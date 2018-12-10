WITH transactions AS (

     SELECT *
     FROM {{ref('netsuite_transactions')}}

),

transaction_lines AS (
     SELECT *
     FROM {{ref('netsuite_transaction_lines')}}
        ),

currencies AS (
     SELECT *
     FROM {{ref('netsuite_currencies')}}
        ),

accounting_periods AS (
     SELECT *
     FROM {{ref('netsuite_accounting_periods')}}
        ),

subsidiaries AS (
     SELECT *
     FROM {{ref('netsuite_subsidiaries')}}
        ),

departments AS (
     SELECT *
     FROM {{ref('netsuite_departments')}}
        )



SELECT
      t.transaction_type,
      t.transaction_date,
      t.posting_period_name,
      t.subsidiary_id,
      t.subsidiary_name,
      t.currency_name                           AS transaction_currency_name,
      t.exchange_rate,
      tl.account_name,
      COALESCE(tl.entity_name, tl.memo)         AS entity,
      c.currency_name,
      ap.posting_period_id,
      d.department_name,
      d.parent_deparment_name,
      COALESCE(SUM(tl.debit_amount), 0.00)      AS debit_amount,
      COALESCE(SUM(tl.credit_amount), 0.00)     AS credit_amount
    FROM transactions t
      JOIN transaction_lines tl     ON t.transaction_id = tl.transaction_id
      JOIN currencies c             ON c.currency_id = t.currency_id
      JOIN accounting_periods ap    ON ap.posting_period_id = t.posting_period_id
      JOIN subsidiaries s           ON s.subsidiary_id = t.subsidiary_id
      LEFT JOIN departments d       ON d.department_id = tl.department_id
    WHERE
          (LEFT (tl.account_name,4) BETWEEN '5000' AND '6999' AND LEFT (tl.account_name,4) != '5079')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13