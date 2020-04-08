WITH source AS (

    SELECT
      *,
      LTRIM(NULLIF(version, ''), 'v')                AS cleaned_version,
      IFF(cleaned_version ILIKE '-pre', True, False) AS version_is_prerelease,
      SPLIT_PART(cleaned_version, '.', 1)::INT       AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::INT       AS minor_version,
      major_version || '.' || minor_version          AS major_minor_version
    FROM {{ ref('version_usage_data_source') }}
    WHERE version NOT LIKE ('%VERSION%') -- Messy data that's not worth parsing.

)

SELECT *
FROM source
