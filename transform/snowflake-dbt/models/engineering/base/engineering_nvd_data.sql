WITH source AS (

    SELECT *
    FROM {{ source('engineering', 'nvd_data') }}

), renamed AS (

    SELECT
      1::INTEGER       AS year,
      2::INTEGER       AS count
    FROM source

)

SELECT *
FROM renamed
