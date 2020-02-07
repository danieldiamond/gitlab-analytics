WITH filtered_attributes AS (

    SELECT
      json_text['']::ARRAY AS file_diffs,
      json_text['']::VARCHAR AS source_branch_name,
      json_text['']::ARRAY AS merge_request_version_diffs,
      json_text['']::VARCHAR AS plain_diff_url_path
    FROM  {{ source('engineering', 'handbook_merge_requests') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY jsontext['plain_diff_path'] 
      ORDER BY ARRAY_SIZE(jsontext['merge_request_diffs']) DESC, uploaded_at DESC) = 1

)
SELECT *
FROM filtered_attributes
 