WITH source AS (
    SELECT *
    FROM {{ source('engineering', 'part_of_product_merge_requests') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY jsontext['plain_diff_path'] ORDER BY ARRAY_SIZE(jsontext['merge_request_diffs']) DESC, uploaded_at DESC) = 1
)
SELECT 
    jsontext['added_lines']::BIGINT      AS added_lines,
    jsontext['real_size']::varchar       AS real_size, --this occasionally has `+` - ie `374+`
    jsontext['removed_lines']::bigint    AS removed_lines,
    jsontext['plain_diff_path']::varchar AS plain_diff_url_path
FROM source