WITH mr_diffs AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_request_diffs') }}

), merge_requests AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_requests') }}

), mr_files AS (
    
    SELECT 
      handbook_file_edited,
      plain_mr_diff_url_path,
      source_branch_name,
      SPLIT(merge_request_version_url_path, 'diff_id=')[1]::BIGINT AS diff_id
    FROM {{ ref('handbook_merge_requests_files') }}

), file_classifications AS (

    SELECT 
      handbook_path,
      file_classification
    FROM {{ ref('handbook_file_classification_mapping') }}

), joined_to_mr AS (

    SELECT 
      merge_requests.merge_request_state        AS merge_request_state,
      merge_requests.updated_at                 AS merge_request_updated_at,
      mr_files.plain_mr_diff_url_path           AS plain_mr_diff_url_path,
      file_classifications.file_classification  AS file_classification
    FROM mr_files
    INNER JOIN mr_diffs
      ON mr_diffs.merge_request_diff_id = mr_files.diff_id
    INNER JOIN merge_requests
      ON mr_diffs.merge_request_id = merge_requests.merge_request_id
    INNER JOIN file_classifications
      ON LOWER(mr_files.handbook_file_edited) LIKE '%' || file_classifications.handbook_path || '%' 

), renamed AS (

    SELECT
      merge_request_state,
      merge_request_updated_at                  AS merge_request_edited_at,
      plain_mr_diff_url_path                    AS plain_merge_request_diff_url_path,
      ARRAY_AGG(DISTINCT file_classification)   AS merge_request_department_list
    FROM joined_to_mr
    GROUP BY 1, 2, 3 

)
SELECT * 
FROM renamed
