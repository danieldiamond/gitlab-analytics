WITH source AS (

    SELECT *
    FROM {{ source('gitlab_data_yaml', 'location_factors') }}

), intermediate AS (

    SELECT d.value                          AS data_by_row,
    date_trunc('day', uploaded_at)::date    AS snapshot_date
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), renamed AS (

    SELECT
      data_by_row['area']::VARCHAR          AS area,
      data_by_row['country']::VARCHAR       AS country,
      data_by_row['locationFactor']::FLOAT  AS location_factor,
      snapshot_date
    FROM intermediate

)

SELECT *
FROM renamed