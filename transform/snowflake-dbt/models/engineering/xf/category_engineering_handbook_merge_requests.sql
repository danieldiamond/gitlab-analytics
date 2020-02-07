WITH diff_id_broken_out AS (
    
    SELECT 
      handbook_file_edited,
      plain_mr_diff_url_path,
      source_branch_name,
      SPLIT(merge_request_version_url_path, 'diff_id=')[1]::BIGINT AS diff_id
    FROM {{ ref('engineering_specific_handbook_merge_requests') }}

), joined_to_mr AS (

    SELECT 
      mr.merge_request_state,
      mr.updated_at,
      di.handbook_file_edited,
      di.plain_mr_diff_url_path,
      CASE WHEN
        di.handbook_file_edited like '%/handbook/engineering/development/%' THEN 'development'
          WHEN
        di.handbook_file_edited like '%/handbook/engineering/infrastructure/%' THEN 'infrastructure'
          WHEN
        di.handbook_file_edited like '%/handbook/engineering/security/%' THEN 'security'
          WHEN
        di.handbook_file_edited like '%/handbook/support/%' THEN 'support'
          WHEN
        di.handbook_file_edited like '%/handbook/engineering/ux/%' THEN 'ux'
          WHEN 
        di.handbook_file_edited like '%/handbook/engineering/%' THEN 'engineering'
          ELSE NULL 
      END AS department

    FROM 
      diff_id_broken_out di
      JOIN
      {{ ref('gitlab_dotcom_merge_request_diffs') }} diff
      ON (diff.merge_request_diff_id = di.DIFF_ID)
      JOIN
      {{ ref('gitlab_dotcom_merge_requests') }} mr
      ON (diff.merge_request_id = mr.merge_request_id)

)
SELECT
  merge_request_state,
  updated_at,
  plain_mr_diff_url_path,
  ARRAY_AGG(DISTINCT department) AS department_list
FROM joined_to_mr
GROUP BY 
  plain_mr_diff_url_path,
  updated_at,
  merge_request_state  
