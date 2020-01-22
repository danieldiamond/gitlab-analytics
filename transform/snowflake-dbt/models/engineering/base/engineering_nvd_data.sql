WITH source AS (

    SELECT *
    FROM {{ source('engineering', 'nvd_data') }}

), renamed AS (

    SELECT
      1       AS year,
      2       AS count
    FROM source

)

SELECT *
FROM renamed
