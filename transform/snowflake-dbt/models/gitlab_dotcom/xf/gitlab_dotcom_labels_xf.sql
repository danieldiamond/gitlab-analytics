WITH labels AS (

	SELECT *
	FROM {{ ref('gitlab_dotcom_labels') }}

),
private_projects AS (

  SELECT
     project_id,
    'not-public' as visibility
  FROM {{ ref('gitlab_dotcom_projects') }}
  WHERE visibility_level != 'public'

),
joined AS (

    SELECT

      label_id,

      CASE
        WHEN private_projects.visibility = 'not-public' THEN 'private/internal project - content masked'
        ELSE label_title
      END                                          as masked_label_title,

      LENGTH(label_title)                          as title_length,
      color,
      labels.project_id,
      group_id,
      template,
      label_type,
      label_created_at,
      label_updated_at

    FROM labels
      LEFT JOIN private_projects on labels.project_id = private_projects.project_id

)

SELECT *
FROM joined