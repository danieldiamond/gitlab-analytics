WITH expenses AS (

     SELECT *
     FROM {{ref('netsuite_expenses')}}

),

transactions AS (
     SELECT *
     FROM {{ref('netsuite_transactions')}}
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
        t.currency_name         AS transaction_currency_name,
        t.exchange_rate,
        e.account_name,
        t.entity_name           AS entity,
        c.currency_name,
        ap.posting_period_id,
        d.department_name,
        d.parent_deparment_name,
        SUM(e.amount)           AS debit_amount,
        SUM(0.00)               AS credit_amount
      FROM expenses e
        JOIN transactions t         ON e.transaction_id = t.transaction_id
        JOIN currencies c           ON c.currency_id = t.currency_id
        JOIN accounting_periods ap  ON ap.posting_period_id = t.posting_period_id
        JOIN subsidiaries s         ON s.subsidiary_id = t.subsidiary_id
        LEFT JOIN departments d     ON d.department_id = e.department_id

      WHERE
      (LEFT (e.account_name,4) BETWEEN '5000' AND '6999' AND LEFT (e.account_name,4) != '5079')
            AND t.entity_name NOT IN ('American Express', 'Comerica Commercial Card Srvc')
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13