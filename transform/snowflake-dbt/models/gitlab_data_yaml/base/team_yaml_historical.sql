WITH source AS (

    SELECT *,
        RANK() OVER (PARTITION BY date_trunc('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'team') }}
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
      data_by_row['gitlab']::varchar        AS gitlab_username,
      data_by_row['name']::varchar          AS name,
      data_by_row['projects']::varchar      AS projects,
      data_by_row['slug']::varchar          AS yaml_slug,
      data_by_row['type']::varchar          AS type,
      snapshot_date
    FROM intermediate

)

SELECT *
FROM renamed