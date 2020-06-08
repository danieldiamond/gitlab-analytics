
WITH issues AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_issues')}}
    WHERE created_at >= DATEADD(year, -1, CURRENT_DATE())

), label_links AS 

    SELECT *
    FROM {{ref('gitlab_dotcom_label_links')}} labels
    WHERE is_currently_valid = True
      AND target_type = 'Issue'

), all_labels AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_labels_xf')}}

), agg_labels AS (

    SELECT
      issues.issue_id,
      ARRAY_AGG(LOWER(masked_label_title)) WITHIN GROUP (ORDER BY masked_label_title ASC) AS labels
    FROM issues
    LEFT JOIN label_links
      ON issues.issue_id = label_links.target_id
    LEFT JOIN all_labels
      ON label_links.label_id = all_labels.label_id
    GROUP BY issues.issue_id

)    

SELECT *
FROM agg_labels