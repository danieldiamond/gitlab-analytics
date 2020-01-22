WITH source AS (

    SELECT *
    FROM {{ source('engineering', 'advisory_data') }}

), renamed AS (

    SELECT
      file              AS filename,
      pubdate::DATE     AS publish_date,
      mergedate::DATE   AS merge_date,
      delta,
      packagetype       AS package_type
    FROM source

)

SELECT *
FROM renamed
