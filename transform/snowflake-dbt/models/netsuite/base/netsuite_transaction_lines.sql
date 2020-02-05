{{ config({
    "materialized": "ephemeral"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'transaction_lines') }}

), renamed AS (

    SELECT
      {{ dbt_utils.surrogate_key('transaction_id', 'transaction_line_id') }}
                                    AS transaction_lines_unique_id,
      --Primary Key
      transaction_id::FLOAT         AS transaction_id,
      transaction_line_id::FLOAT    AS transaction_line_id,

      --Foreign Keys
      account_id::FLOAT             AS account_id,
      class_id::FLOAT               AS class_id,
      department_id::FLOAT          AS department_id,
      subsidiary_id::FLOAT          AS subsidiary_id,
      company_id::FLOAT             AS company_id,

      -- info
      memo::VARCHAR                 AS memo,
      receipt_url::VARCHAR          AS receipt_url,
      amount::FLOAT                 AS amount,
      gross_amount::FLOAT           AS gross_amount

    FROM source
    WHERE LOWER(non_posting_line) != 'yes' --removes transactions not intended for posting
)

SELECT *
FROM renamed
