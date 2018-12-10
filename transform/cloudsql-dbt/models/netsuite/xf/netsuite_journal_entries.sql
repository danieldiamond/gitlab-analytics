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
        ),

consolidated_exchange_rates AS (
     SELECT *
     FROM {{ref('netsuite_consolidated_exchange_rates')}}
        ),

journal_entries AS (
    SELECT
          t.transaction_type,
          t.transaction_date,
          t.period_date,
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
        )

SELECT
    e.transaction_type,
    e.transaction_date,
    e.period_date,
    e.subsidiary_id,
    e.subsidiary_name,
    e.transaction_currency_name,
    e.exchange_rate,
    e.account_name,
    e.entity,
    e.currency_name,
    e.posting_period_id,
    e.department_name,
    e.parent_deparment_name,
    e.exchange_rate * e.debit_amount  AS debit_amount,
    e.exchange_rate * e.credit_amount AS credit_amount,
     CASE WHEN e.subsidiary_name = 'GitLab Inc'
       THEN 1
     ELSE r.average_rate END AS consolidated_exchange
   FROM journal_entries e
     LEFT JOIN consolidated_exchange_rates r
       ON CAST(r.posting_period_id AS INTEGER) = e.posting_period_id
              AND CAST(r.from_subsidiary_id AS INT) = e.subsidiary_id