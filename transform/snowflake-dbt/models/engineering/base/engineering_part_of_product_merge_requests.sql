{{
    config({
    "schema": "staging"
    })
}}

WITH source AS (
    
    SELECT *
    FROM {{ source('engineering', 'part_of_product_merge_requests') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY jsontext['plain_diff_path'] 
      ORDER BY ARRAY_SIZE(jsontext['merge_request_diffs']) DESC, uploaded_at DESC) = 1

), renamed AS (
    
    SELECT 
      jsontext['added_lines']::BIGINT      AS added_lines,
      jsontext['real_size']::VARCHAR       AS real_size, --this occasionally has `+` - ie `374+`
      jsontext['removed_lines']::BIGINT    AS removed_lines,
      jsontext['plain_diff_path']::VARCHAR AS plain_diff_url_path
    FROM source

)
SELECT * 
FROM renamed

