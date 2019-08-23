WITH transactions AS (
     SELECT *
     FROM {{ref('netsuite_stitch_transactions_xf')}}
),

transaction_lines AS (
     SELECT *
     FROM {{ref('netsuite_stitch_transaction_line_list')}}
),

accounting_periods AS (
     SELECT *
     FROM {{ref('netsuite_stitch_accounting_periods')}}
),

accounts AS (
     SELECT *
     FROM {{ref('netsuite_stitch_account_xf')}}
),

exchange_rates AS (
     SELECT *
     FROM {{ref('netsuite_stitch_currency_rate')}}
),

subsidiaries AS (
     SELECT *
     FROM {{ref('netsuite_stitch_subsidiary')}}
),

departments AS (
     SELECT *
     FROM {{ref('netsuite_stitch_department')}}
),

consolidated_exchange_rates AS (
     SELECT *
     FROM {{ref('netsuite_stitch_consolidated_exchange_rates')}}
),

journal_entries AS (

    SELECT
          t.transaction_id,
          t.transaction_type,
          t.transaction_date,
          t.period_date,
          t.subsidiary_id,
          t.subsidiary_name,
          t.currency_name                           AS transaction_currency_name,
          IFF(t.exchange_rate IS NULL,
            er.exchange_rate,
            t.exchange_rate)                        AS exchange_rate,
          tl.account_name,
          a.unique_account_code                     AS unique_account_code,
          a.account_code                            AS account_code,
          a.ultimate_account_code                   AS ultimate_account_code,
          COALESCE(tl.entity_name, tl.memo)         AS entity,
          ap.accounting_period_id                   AS posting_period_id,
          d.department_name,
          COALESCE(d.parent_name, 'zNeed Accounting Reclass') AS parent_department_name,
          COALESCE(SUM(tl.debit), 0.00)             AS debit_amount,
          COALESCE(SUM(tl.credit), 0.00)            AS credit_amount
        FROM transactions t
        JOIN transaction_lines tl     ON t.transaction_id = tl.transaction_id
        JOIN accounting_periods ap    ON ap.accounting_period_id = t.posting_period_id
        JOIN accounts a               ON tl.account_id = a.account_id
        JOIN subsidiaries s           ON t.subsidiary_id = s.subsidiary_id
        LEFT JOIN departments d       ON d.department_id = tl.department_id
        LEFT JOIN exchange_rates  er       ON DATE_TRUNC(DAY, t.transaction_date) = DATE_TRUNC(DAY, er.effective_date)
          AND t.currency_id = er.transaction_currency_id
          AND s.currency_name = er.base_currency_name
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16

), final as (

        SELECT
            e.transaction_id,
            CASE WHEN e.transaction_type IS NULL
               THEN 'JournalEntry'
              ELSE e.transaction_type
            END AS transaction_type,
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
            CASE WHEN e.subsidiary_name = 'GitLab Inc'
              THEN 1
             ELSE r.average_rate
            END AS consolidated_exchange,
            IFF(e.exchange_rate IS NULL, 1, e.exchange_rate) AS exchange_factor,
            consolidated_exchange * e.debit_amount * exchange_factor AS debit_amount,
            consolidated_exchange * e.credit_amount * exchange_factor AS credit_amount
        FROM journal_entries e
        LEFT JOIN consolidated_exchange_rates r
         ON CAST(r.postingperiod AS INTEGER) = e.posting_period_id
         AND CAST(r.from_subsidiary AS INTEGER) = e.subsidiary_id

)

SELECT *
FROM final
