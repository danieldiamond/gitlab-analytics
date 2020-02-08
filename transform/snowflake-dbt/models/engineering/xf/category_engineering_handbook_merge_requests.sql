WITH gitlab_dotcom_mr_diffs AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_request_diffs') }}

), gitlab_dotcom_merge_requests AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_requests') }}

), diff_id_broken_out AS (
    
    SELECT 
      handbook_file_edited,
      plain_mr_diff_url_path,
      source_branch_name,
      SPLIT(merge_request_version_url_path, 'diff_id=')[1]::BIGINT AS diff_id
    FROM {{ ref('engineering_specific_handbook_merge_requests') }}

), joined_to_mr AS (

    SELECT 
      gitlab_dotcom_merge_requests.merge_request_state    AS gitlab_db_merge_request_state,
      gitlab_dotcom_merge_requests.updated_at             AS gitlab_db_merge_request_updated_at,
      diff_id_broken_out.plain_mr_diff_url_path           AS gitlab_api_plain_mr_diff_url_path,
      CASE 
        WHEN diff_id_broken_out.handbook_file_edited like '%/handbook/engineering/development/%' 
          THEN 'development'
        WHEN diff_id_broken_out.handbook_file_edited like '%/handbook/engineering/infrastructure/%' 
          THEN 'infrastructure'
        WHEN diff_id_broken_out.handbook_file_edited like '%/handbook/engineering/security/%' 
          THEN 'security'
        WHEN diff_id_broken_out.handbook_file_edited like '%/handbook/support/%' 
          THEN 'support'
        WHEN diff_id_broken_out.handbook_file_edited like '%/handbook/engineering/ux/%' 
          THEN 'ux'
        WHEN diff_id_broken_out.handbook_file_edited like '%/handbook/engineering/%' 
          THEN 'engineering'
        ELSE NULL 
      END                       AS gitlab_api_merge_request_file_department
    FROM 
      diff_id_broken_out
      INNER JOIN
      gitlab_dotcom_mr_diffs
      ON (gitlab_dotcom_mr_diffs.merge_request_diff_id = diff_id_broken_out.diff_id)
      INNER JOIN
      gitlab_dotcom_merge_requests
      ON (gitlab_dotcom_mr_diffs.merge_request_id = gitlab_dotcom_merge_requests.merge_request_id)

), renamed AS (

    SELECT
      gitlab_db_merge_request_state                                AS merge_request_state,
      gitlab_db_merge_request_updated_at                           AS merge_request_edited_at,
      gitlab_api_plain_mr_diff_url_path                            AS plain_merge_request_diff_url_path,
      ARRAY_AGG(DISTINCT gitlab_api_merge_request_file_department) AS merge_request_department_list
    FROM joined_to_mr
    GROUP BY 1, 2, 3 

)
SELECT * 
FROM renamed