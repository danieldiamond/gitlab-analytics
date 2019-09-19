WITH source AS (

    SELECT *,
        RANK() OVER (PARTITION BY date_trunc('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'roles') }}
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
      data_by_row['salary']::number         AS salary,
      data_by_row['title']::varchar         AS title,
      data_by_row['levels']::varchar        AS role_levels,
      data_by_row['open']::varchar          AS is_open,
      snapshot_date
    FROM intermediate

)

SELECT *
FROM renamed