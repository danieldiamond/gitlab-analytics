{{ config({
    "schema": "staging"
    })
}}

WITH items AS (
     SELECT *
     FROM {{ref('netsuite_stitch_transaction_item_list')}}
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

),non_journal_entries_items AS (

    SELECT  t.transaction_id,
            t.transaction_type,
            t.transaction_date,
            t.period_date,
            t.subsidiary_id,
            t.subsidiary_name,
            t.currency_name                                              AS transaction_currency_name,
            t.exchange_rate,
            i.account_name                                               AS balance_sheet_account_name,
            a.unique_account_code                                        AS balance_sheet_unique_account_code,
            a.account_code                                               AS balance_sheet_account_code,
            a.ultimate_account_code                                      AS balance_sheet_ultimate_account_code,
            CASE WHEN item_name = 'GCP Trial Referrals' THEN 4008
                 WHEN item_name = 'GitHost' THEN 4000
            END                                                          AS income_statement_account_code,
            t.entity_name                                                AS entity,
            ap.accounting_period_id                                      AS posting_period_id,
            d.department_name,
            COALESCE(d.parent_name, 'zNeed Accounting Reclass')          AS parent_department_name,
            SUM(i.amount)                                                AS amount
    FROM items i
    INNER JOIN transactions t         ON i.transaction_id = t.transaction_id
    INNER JOIN accounting_periods ap  ON ap.accounting_period_id = t.posting_period_id
    INNER JOIN accounts a             ON i.account_id = a.account_id
    LEFT JOIN departments d           ON d.department_id = i.department_id
    {{ dbt_utils.group_by(n=17) }}

), final as (

    SELECT i.transaction_id,
           i.transaction_type,
           i.transaction_date,
           i.period_date,
           i.subsidiary_id,
           i.subsidiary_name,
           i.transaction_currency_name,
           i.exchange_rate,
           i.balance_sheet_account_name,
           i.balance_sheet_unique_account_code,
           i.balance_sheet_account_code,
           i.balance_sheet_ultimate_account_code,
           i.income_statement_account_code,
           CASE WHEN income_statement_account_code = 4008 THEN '4008 - Revenue - GCP Referral'
                WHEN income_statement_account_code = 4000 THEN '4000 Revenue - Hosting'
           END                                                              AS income_statement_account_name,
           i.entity,
           i.posting_period_id,
           i.department_name,
           i.parent_department_name,
           CASE WHEN i.subsidiary_name = 'GitLab Inc'
             THEN 1 ELSE r.average_rate
           END                                                              AS consolidated_exchange,
           IFF(i.exchange_rate IS NULL, 1, i.exchange_rate)                 AS exchange_factor,
           CASE WHEN income_statement_account_code in (4000,4008)
                THEN consolidated_exchange * i.amount * exchange_factor END AS income_statement_credit_amount,
           CASE WHEN balance_sheet_account_code in (1101)
                THEN consolidated_exchange * i.amount * exchange_factor END AS balance_sheet_debit_amount

    FROM non_journal_entries_items i
    LEFT JOIN consolidated_exchange_rates r
        ON CAST(r.postingperiod AS INTEGER) = i.posting_period_id
        AND CAST(r.from_subsidiary AS INTEGER) = i.subsidiary_id
)

SELECT *
FROM final
