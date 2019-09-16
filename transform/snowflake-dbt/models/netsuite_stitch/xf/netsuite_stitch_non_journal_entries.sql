{{ config({
    "schema": "staging"
    })
}}

WITH expenses AS (
     SELECT *
     FROM {{ref('netsuite_stitch_transaction_expense_list')}}
),

transactions AS (
     SELECT *
     FROM {{ref('netsuite_stitch_transactions_xf')}}
),

accounting_periods AS (
     SELECT *
     FROM {{ref('netsuite_stitch_accounting_periods')}}
),

accounts AS (
     SELECT *
     FROM {{ref('netsuite_stitch_account_xf')}}
),

departments AS (
     SELECT *
     FROM {{ref('netsuite_stitch_department')}}
),

consolidated_exchange_rates AS (
     SELECT *
     FROM {{ref('netsuite_stitch_consolidated_exchange_rates')}}
),

non_journal_entries AS (

    SELECT
            t.transaction_id,
            t.transaction_type,
            t.transaction_date,
            t.period_date,
            t.subsidiary_id,
            t.subsidiary_name,
            t.currency_name         AS transaction_currency_name,
            t.exchange_rate,
            e.account_name,
            a.unique_account_code                     AS unique_account_code,
            a.account_code                            AS account_code,
            a.ultimate_account_code                   AS ultimate_account_code,
            CASE
              WHEN e.account_name ILIKE '%Contract%'
                THEN substring(md5(t.entity_name),16)
              ELSE t.entity_name END AS entity,
            ap.accounting_period_id                   AS posting_period_id,
            d.department_name,
            COALESCE(d.parent_name, 'zNeed Accounting Reclass') AS parent_department_name,
            SUM(e.amount)           AS amount
          FROM expenses e
            JOIN transactions t         ON e.transaction_id = t.transaction_id
            JOIN accounting_periods ap  ON ap.accounting_period_id = t.posting_period_id
            JOIN accounts a               ON e.account_id = a.account_id
            LEFT JOIN departments d     ON d.department_id = e.department_id
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16

        ), final as (

    SELECT
        e.transaction_id,
        e.transaction_type,
        e.transaction_date,
        e.period_date,
        e.subsidiary_id,
        e.subsidiary_name,
        e.transaction_currency_name,
        e.exchange_rate,
        e.account_name,
        e.account_code,
        e.unique_account_code,
        e.ultimate_account_code,
        e.entity,
        e.posting_period_id,
        e.department_name,
        e.parent_department_name,
        CASE
         WHEN e.subsidiary_name = 'GitLab Inc'
          THEN 1
         ELSE r.average_rate
        END AS consolidated_exchange,
        IFF(e.exchange_rate IS NULL, 1, e.exchange_rate) AS exchange_factor,
        IFF(e.transaction_type != 'VendorCredit' AND e.amount >= 0, consolidated_exchange * e.amount * exchange_factor, 0) AS debit_amount,
        IFF(e.transaction_type = 'VendorCredit' OR e.amount < 0, ABS(consolidated_exchange * e.amount * exchange_factor), 0) AS credit_amount
    FROM non_journal_entries e
    LEFT JOIN consolidated_exchange_rates r
         ON CAST(r.postingperiod AS INTEGER) = e.posting_period_id
         AND CAST(r.from_subsidiary AS INTEGER) = e.subsidiary_id
    )

SELECT *
FROM final
