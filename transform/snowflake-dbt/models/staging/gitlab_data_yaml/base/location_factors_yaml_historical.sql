WITH source AS (

    SELECT *,
        RANK() OVER (PARTITION BY date_trunc('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'location_factors') }}
    ORDER BY uploaded_at DESC

), filtered as (

    SELECT *
    FROM source
    WHERE rank = 1

), intermediate AS (

    SELECT d.value                          AS data_by_row,
    date_trunc('day', uploaded_at)::date    AS snapshot_date
    FROM filtered,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), renamed AS (

    SELECT
      data_by_row['area']::varchar          AS area,
      data_by_row['country']::varchar       AS country,
      data_by_row['locationFactor']::float  AS location_factor,
      snapshot_date
    FROM intermediate

)

SELECT *
FROM renamed