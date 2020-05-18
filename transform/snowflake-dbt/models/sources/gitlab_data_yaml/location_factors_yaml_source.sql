WITH source AS (

    SELECT *,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'location_factors') }}

), intermediate AS (

    SELECT d.value                          AS data_by_row,
    date_trunc('day', uploaded_at)::date    AS snapshot_date,
    rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), renamed AS (

    SELECT
      data_by_row['area']::VARCHAR          AS area,
      data_by_row['country']::VARCHAR       AS country,
      data_by_row['locationFactor']::FLOAT  AS location_factor,
      snapshot_date,
      rank
    FROM intermediate

)

SELECT *
FROM renamed