WITH base AS (

    SELECT *
    FROM {{ ref('version_versions_source') }}

)

SELECT *
FROM base