{{ config({
    "schema": "analytics"
    })
}}
WITH income_entries AS (

    SELECT transaction_id,
           transaction_type,
           transaction_date,
           period_date,
           subsidiary_id,
           subsidiary_name,
           transaction_currency_name,
           exchange_rate,
           account_name,
           account_code,
           unique_account_code,
           ultimate_account_code,
           entity,
           posting_period_id,
           department_name,
           parent_department_name,
           consolidated_exchange,
           exchange_factor,
           debit_amount,
           credit_amount

    FROM {{ref('netsuite_stitch_journal_entries')}}
    WHERE (ultimate_account_code BETWEEN 4000 AND 4010)

UNION ALL

    SELECT transaction_id,
           transaction_type,
           transaction_date,
           period_date,
           subsidiary_id,
           subsidiary_name,
           transaction_currency_name,
           exchange_rate,
           income_statement_account_name   AS account_name,
           income_statement_account_code   AS account_code,
           '0'                             AS unique_account_code,
           '0'                             AS ultimate_account_code,
           entity,
           posting_period_id,
           department_name,
           parent_department_name,
           consolidated_exchange,
           exchange_factor,
           '0'                             AS debit_amount,
           income_statement_credit_amount  AS credit_amount
FROM {{ref('netsuite_stitch_non_journal_balance_sheet_item')}}
WHERE (income_statement_account_code BETWEEN 4000 and 4010)

)

SELECT *
FROM income_entries
