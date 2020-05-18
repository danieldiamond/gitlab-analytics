WITH source AS (

    SELECT *
    FROM {{ ref('location_factors_yaml_source') }}
    ORDER BY uploaded_at DESC

), filtered as (

    SELECT *
    FROM source
    WHERE rank = 1

)

SELECT *
FROM filtered