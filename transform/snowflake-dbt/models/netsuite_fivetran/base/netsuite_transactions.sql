WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'transactions') }}

), renamed AS (

    SELECT transaction_id,
           entity_id,
           accounting_period_id,
           currency_id,
           transaction_type,
           external_ref_number         AS external_transaction_id,
           transaction_number,
           memo,
           opening_balance_transaction AS balance,
           exchange_rate,
           weighted_total              AS total,
           status,
           due_date::date              AS due_date,
           trandate::date              AS transaction_date,
           sales_effective_date::date  AS sales_effective_date

    FROM source

)

SELECT *
FROM renamed
