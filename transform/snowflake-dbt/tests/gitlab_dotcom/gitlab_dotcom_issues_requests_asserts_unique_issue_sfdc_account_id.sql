-- fail if tuple (issue_id, sfdc_account_id) is not unique

WITH row_count_calc AS (

  SELECT
    issue_id,
    sfdc_account_id,
    COUNT(*)AS row_count
  FROM {{ ref('gitlab_dotcom_gitlab_issues_requests') }}
  GROUP BY 1,2

)

SELECT *
FROM row_count_calc
WHERE row_count > 1
