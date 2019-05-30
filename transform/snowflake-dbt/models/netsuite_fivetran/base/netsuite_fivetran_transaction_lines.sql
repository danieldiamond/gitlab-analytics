{{
    config({
        "schema": "sensitive"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('netsuite_fivetran', 'transaction_lines') }}

), renamed AS (

    SELECT {{ dbt_utils.surrogate_key('transaction_id', 'transaction_line_id') }} AS transaction_lines_unique_id,
          -- keys
          transaction_id,
          transaction_line_id,
          account_id,
          department_id,
          subsidiary_id,

          -- info
          memo as memo,
          amount,
          gross_amount

    FROM source

)

SELECT *
FROM renamed
