{{ config({
    "materialized": "ephemeral"
    })
}}

WITH base AS (

    SELECT *
    FROM {{ ref('handbook_merge_requests') }}

), exploded_file_paths AS ( -- explodes the files in the list of diffs
    
    SELECT 
      file_diffs.value:file_path::VARCHAR AS handbook_merge_request_file_edited,
      base.plain_diff_url_path            AS plain_diff_url_path,
      base.merge_request_version_diffs    AS merge_request_version_diffs,
      base.source_branch_name             AS source_branch_name
    FROM base
    INNER JOIN TABLE(FLATTEN(INPUT => file_diffs, outer => true)) AS file_diffs
    WHERE LOWER(file_diffs.value:file_path) LIKE '%/handbook/%'

), current_mr_diff AS ( -- keeps only the latest MR diff

    SELECT 
      exploded_file_paths.handbook_merge_request_file_edited  AS handbook_file_edited,
      exploded_file_paths.plain_diff_url_path                 AS plain_mr_diff_url_path,
      exploded_file_paths.source_branch_name                  AS source_branch_name,
      mr_diffs.value:version_path::VARCHAR                    AS merge_request_version_url_path
    FROM exploded_file_paths
    INNER JOIN TABLE(FLATTEN(INPUT => merge_request_version_diffs, outer => true)) AS mr_diffs
    WHERE mr_diffs.value:latest::BOOLEAN

)

SELECT * 
FROM current_mr_diff
