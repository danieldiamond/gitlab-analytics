WITH source AS (

    SELECT *
    FROM {{ source('netsuite_stitch', 'currency_rate') }}

), renamed AS (

    SELECT
        internalid                                  AS currency_rate_id,
        basecurrency['internalId']::NUMBER          AS base_currency_id,
        transactioncurrency['internalId']::NUMBER   AS transaction_currency_id,

        exchangerate                                AS exchange_rate,
        basecurrency['name']::STRING                AS base_currency_name,
        transactioncurrency['name']::STRING         AS transaction_currency_name,
        effectivedate                               AS effective_date
    FROM source

)

SELECT *
FROM renamed