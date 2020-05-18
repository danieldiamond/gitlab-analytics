WITH source AS (

    SELECT 
      *
    FROM {{ ref('roles_yaml_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) = 1

)

SELECT *
FROM source
