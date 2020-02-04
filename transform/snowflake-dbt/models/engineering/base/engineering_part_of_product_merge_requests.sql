WITH source AS (
    SELECT *
    FROM {{ source('engineering', 'part_of_product_merge_requests') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY JSONTEXT['plain_diff_path'] ORDER BY uploaded_at DESC) = 1
)
SELECT 
    JSONTEXT['added_lines']::bigint      AS added_lines,
    JSONTEXT['real_size']::varchar       AS real_size, --this occasionally has `+` - ie `374+`
    JSONTEXT['removed_lines']::bigint    AS removed_lines,
    JSONTEXT['plain_diff_path']::varchar AS plain_diff_url_path
FROM source