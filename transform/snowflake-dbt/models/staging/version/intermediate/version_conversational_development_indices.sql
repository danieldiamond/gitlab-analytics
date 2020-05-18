WITH base AS (

    SELECT *
    FROM {{ ref('version_conversational_development_indices_source') }}

)

SELECT *
FROM base