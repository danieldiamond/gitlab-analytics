WITH source AS (

    SELECT *
    FROM {{ source('engineering', 'nvd_data') }}

), renamed AS (

    SELECT
      "0"::INTEGER       AS year,
      "1"::INTEGER       AS count
    FROM source

)

SELECT *
FROM renamed
