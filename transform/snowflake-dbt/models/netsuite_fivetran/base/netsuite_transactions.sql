WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'transactions') }}

), renamed AS (

    SELECT transaction_id::float                AS transaction_id,
           entity_id::float                     AS entity_id,
           accounting_period_id::float          AS accounting_period_id,
           currency_id::float                   AS currency_id,
           transaction_type::varchar            AS transaction_type,
           external_ref_number::varchar         AS external_transaction_id,
           transaction_number::varchar          AS transaction_number,
           memo::varchar                        AS memo,
           opening_balance_transaction::varchar AS balance,
           exchange_rate::float                 AS exchange_rate,
           weighted_total::float                AS total,
           status::varchar                      AS status,
           due_date::timestamp_tz               AS due_date,
           trandate::timestamp_tz               AS transaction_date,
           sales_effective_date::timestamp_tz   AS sales_effective_date

    FROM source

)

SELECT *
FROM renamed
