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
        ),

consolidated_exchange_rates AS (
     SELECT *
     FROM {{ref('netsuite_consolidated_exchange_rates')}}
        ),

non_journal_entries AS (

    SELECT
            t.transaction_type,
            t.transaction_date,
            t.period_date,
            t.subsidiary_id,
            t.subsidiary_name,
            t.currency_name         AS transaction_currency_name,
            t.exchange_rate,
            e.account_name,
            LEFT (e.account_name,4) AS account_code,
            CASE
              WHEN e.account_name ILIKE '%Contract%'
                THEN substring(md5(t.entity_name),16)
              ELSE t.entity_name END AS entity,
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
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14

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
    e.account_code,
    e.entity,
    e.currency_name,
    e.posting_period_id,
    e.department_name,
    e.parent_deparment_name,
    CASE WHEN (subsidiary_name != 'GitLab Inc' OR currency_name != 'US Dollar')
        THEN exchange_rate * e.debit_amount
    ELSE e.debit_amount END             AS debit_amount,
    CASE WHEN (subsidiary_name != 'GitLab Inc' OR currency_name != 'US Dollar')
        THEN exchange_rate * e.credit_amount
    ELSE e.credit_amount END            AS credit_amount,
    COALESCE(r.average_rate, 1)         AS consolidated_exchange
FROM non_journal_entries e
  LEFT JOIN consolidated_exchange_rates r
    ON CAST(r.posting_period_id AS INTEGER) = e.posting_period_id
       AND CAST(r.from_subsidiary_id AS INTEGER) = e.subsidiary_id
