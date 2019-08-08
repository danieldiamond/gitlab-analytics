{{ config({
    "schema": "analytics"
    })
}}
WITH income_entries AS (

    SELECT *
    FROM {{ref('netsuite_stitch_journal_entries')}}
    WHERE (ultimate_account_code BETWEEN 4000 AND 4010)

)

SELECT
    transaction_id,
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
    credit_amount,
    debit_amount

FROM income_entries
