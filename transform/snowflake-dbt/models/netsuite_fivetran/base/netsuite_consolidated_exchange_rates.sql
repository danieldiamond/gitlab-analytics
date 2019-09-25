with source AS (

    SELECT *
    FROM {{ source('netsuite_fivetran', 'consolidated_exchange_rates') }}

), renamed AS (

    SELECT consolidated_exchange_rate_id,

           accounting_book_id,
           accounting_period_id,

           average_budget_rate,
           current_budget_rate,

           average_rate,
           current_rate,

           historical_budget_rate,
           historical_rate,

           from_subsidiary_id,
           to_subsidiary_id

    FROM source
    WHERE _fivetran_deleted = 'False'
)

SELECT *
FROM renamed
