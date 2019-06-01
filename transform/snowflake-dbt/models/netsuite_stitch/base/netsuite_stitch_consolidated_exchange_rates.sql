WITH source AS (

    SELECT *
    FROM {{ source('netsuite_stitch', 'consolidated_exchange_rate') }}

), renamed AS (

    SELECT
        internalid                  AS consolidated_exchnage_rate_id,
        currentrate                 AS current_rate,
        averagerate                 AS average_rate,
        fromsubsidiary              AS from_subsidiary,
        postingperiod               AS postingperiod,
        isderived                   AS is_derived,
        historicalrate              AS historical_rate,
        isperiodclosed              AS is_period_closed,
        tocurrency                  AS to_currency,
        tosubsidiary                AS to_subsidiary,
        fromcurrency                AS from_currency,
        iseliminationsubsidiary     AS is_elimination_subsidiary
    FROM source

)

SELECT *
FROM renamed


