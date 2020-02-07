WITH exploded_file_paths AS (
    
    SELECT 
      d.file_path AS handbook_merge_request_file_edited,
      base.plain_diff_url_path,
      base.merge_request_version_diffs,
      base.source_branch_name
    FROM {{ ref('engineering', 'engineering_handbook_merge_requests') }},
    LATERAL FLATTEN(INPUT => parse_json(file_diffs), outer => true) d
    WHERE d.file_path LIKE ANY ('%/handbook/engineering/%', '%/handbook/support/%') 

), current_mr_diff AS (

    SELECT 
      fp.handbook_merge_request_file_edited  AS handbook_file_edited,
      fp.plain_diff_url_path                 AS plain_mr_diff_url_path,
      fp.source_branch_name                  AS source_branch_name
      d.version_path                         AS merge_request_version_url_path
    FROM exploded_file_paths,
    LATERAL FLATTEN(INPUT => parse_json(merge_request_version_diffs), outer => true) d
    WHERE d.latest

)
SELECT * FROM current_mr_diff