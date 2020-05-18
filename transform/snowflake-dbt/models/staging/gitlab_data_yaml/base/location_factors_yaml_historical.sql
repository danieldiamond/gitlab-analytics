WITH source AS (

    SELECT *,
        RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ ref('location_factors_yaml_source') }}
    ORDER BY uploaded_at DESC

), filtered as (

    SELECT *
    FROM source
    WHERE rank = 1

)

SELECT *
FROM filtered