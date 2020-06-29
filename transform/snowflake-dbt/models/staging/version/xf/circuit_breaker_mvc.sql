WITH usage_data_unpacked AS (

    SELECT *
    FROM {{  ref('version_usage_data_unpacked') }}

), transformed AS (

    SELECT
      *,
      LEAD(id) OVER (PARTITION BY uuid ORDER BY created_at) AS last_ping_id
    FROM usage_data_unpacked

),

joined AS (

    SELECT 
        transformed.*,
        DATEDIFF('day', usage_data_unpacked.created_at, transformed.created_at) AS date_diff_with_former_ping
    FROM transformed
    LEFT JOIN usage_data_unpacked ON transformed.last_ping_id = usage_data_unpacked.id
)

SELECT *
FROM joined
