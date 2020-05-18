WITH source AS (

    SELECT *,
      RANK() OVER (PARTITION BY date_trunc('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ ref('team_yaml_source') }}

), filtered as (

    SELECT *
    FROM source
    WHERE rank = 1

)

SELECT *
FROM filtered