WITH diff_id_broken_out AS (
    
    SELECT 
      handbook_file_edited,
      plain_mr_diff_url_path,
      source_branch_name,
      SPLIT(merge_request_version_url_path, 'diff_id=')[1]::BIGINT AS diff_id
    FROM {{ ref('engineering_specific_handbook_merge_requests') }}

), joined_to_mr AS (

    SELECT 
      mr.merge_request_state    AS gitlab_db_merge_request_state,
      mr.updated_at             AS gitlab_db_merge_request_updated_at,
      di.plain_mr_diff_url_path AS gitlab_api_plain_mr_diff_url_path,
      CASE 
        WHEN di.handbook_file_edited like '%/handbook/engineering/development/%' 
          THEN 'development'
        WHEN di.handbook_file_edited like '%/handbook/engineering/infrastructure/%' 
          THEN 'infrastructure'
        WHEN di.handbook_file_edited like '%/handbook/engineering/security/%' 
          THEN 'security'
        WHEN di.handbook_file_edited like '%/handbook/support/%' 
          THEN 'support'
        WHEN di.handbook_file_edited like '%/handbook/engineering/ux/%' 
          THEN 'ux'
        WHEN di.handbook_file_edited like '%/handbook/engineering/%' 
          THEN 'engineering'
        ELSE NULL 
      END                       AS gitlab_api_merge_request_file_department

    FROM 
      diff_id_broken_out AS di
      INNER JOIN
      {{ ref('gitlab_dotcom_merge_request_diffs') }} AS diff
      ON (diff.merge_request_diff_id = di.DIFF_ID)
      INNER JOIN
      {{ ref('gitlab_dotcom_merge_requests') }} AS mr
      ON (diff.merge_request_id = mr.merge_request_id)

), renamed AS (

    SELECT
      gitlab_db_merge_request_state                                AS merge_request_state,
      gitlab_db_merge_request_updated_at                           AS merge_request_edited_at,
      gitlab_api_plain_mr_diff_url_path                            AS plain_merge_request_diff_url_path,
      ARRAY_AGG(DISTINCT gitlab_api_merge_request_file_department) AS merge_request_department_list
    FROM joined_to_mr
    GROUP BY 1, 2, 3 

)
SELECT * FROM renamed