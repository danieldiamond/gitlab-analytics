WITH source AS (

    SELECT
      *,
      LTRIM(version, 'v')                           AS cleaned_version,
      SPLIT_PART(cleaned_version, '.', 1)::INT      AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::INT      AS minor_version,
      major_version || '.' || minor_version         AS major_minor_version
    FROM {{ ref('version_usage_data_source') }}
    WHERE uuid NOT IN (
      '13880013', -- Bad `version` values
      '13718000',
      '14040899'
    )

)

SELECT *
FROM source
