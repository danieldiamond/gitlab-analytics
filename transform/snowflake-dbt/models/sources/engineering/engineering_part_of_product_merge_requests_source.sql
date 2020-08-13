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
      jsontext['added_lines']::NUMBER                             AS added_lines,
      jsontext['real_size']::VARCHAR                              AS real_size, --this occasionally has `+` - ie `374+`
      jsontext['removed_lines']::NUMBER                           AS removed_lines,
      jsontext['plain_diff_path']::VARCHAR                        AS plain_diff_url_path,
      jsontext['merge_request_diff']['created_at']::TIMESTAMP     AS merge_request_updated_at,
      jsontext['diff_files']::ARRAY                               AS file_diffs,
      jsontext['target_branch_name']                              AS target_branch_name,
      --get the number after the last dash
      REGEXP_REPLACE(
          GET(SPLIT(plain_diff_url_path, '-'), ARRAY_SIZE(SPLIT(plain_diff_url_path, '-')) - 1),  
          '[^0-9]+', 
          ''
      )::NUMBER                                                   AS product_merge_request_iid,
      TRIM(ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(plain_diff_url_path, '-'), 0, -1), '-'), '/')::VARCHAR AS product_merge_request_project
    FROM source

)
SELECT * 
FROM renamed

