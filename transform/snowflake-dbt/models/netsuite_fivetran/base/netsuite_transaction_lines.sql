{{ config({
    "materialized": "ephemeral"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'transaction_lines') }}

), renamed AS (

    SELECT {{ dbt_utils.surrogate_key('transaction_id', 'transaction_line_id') }} AS transaction_lines_unique_id,
          -- keys
          transaction_id::float         AS transaction_id,
          transaction_line_id::float    AS transaction_line_id,
          account_id::float             AS account_id,
          department_id::float          AS department_id,
          subsidiary_id::float          AS subsidiary_id,

          -- info
          memo::varchar                 AS memo,
          amount::float                 AS amount,
          gross_amount::float           AS gross_amount

    FROM source

)

SELECT *
FROM renamed
