with source AS (

    SELECT *
    FROM {{ source('netsuite', 'consolidated_exchange_rates') }}

), renamed AS (

    SELECT consolidated_exchange_rate_id::float   AS consolidated_exchange_rate_id,

           accounting_book_id::float              AS accounting_book_id,
           accounting_period_id::float            AS accounting_period_id,

           average_budget_rate::float             AS average_budget_rate,
           current_budget_rate::float             AS current_budget_rate,

           average_rate::float                    AS average_rate,
           current_rate::float                    AS current_rate,

           historical_budget_rate::float          AS historical_budget_rate,
           historical_rate::float                 AS historical_rate,

           from_subsidiary_id::float              AS from_subsidiary_id,
           to_subsidiary_id::float                AS to_subsidiary_id

    FROM source
    WHERE _fivetran_deleted = 'False'
)

SELECT *
FROM renamed
