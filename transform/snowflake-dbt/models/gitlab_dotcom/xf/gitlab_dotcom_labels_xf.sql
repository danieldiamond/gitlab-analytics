with labels as (

  SELECT *
  FROM {{ ref('gitlab_dotcom_labels') }}

), projects as (

  SELECT project_id,
         visibility_level,
         namespace_id
  FROM {{ ref('gitlab_dotcom_projects') }}

), internal_namespaces as (

    SELECT
      namespace_id
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}
    WHERE COALESCE(ultimate_parent_id, namespace_id) IN {{ get_internal_parent_namespaces() }}
), joined as (

    SELECT label_id,

           CASE
             WHEN projects.visibility_level != 'public' AND namespace_id IN (SELECT * FROM internal_namespaces) THEN 'content masked'
             ELSE label_title
           END                                          AS masked_label_title,

           LENGTH(label_title)                          AS title_length,
           color,
           labels.project_id,
           group_id,
           template,
           label_type,
           label_created_at,
          label_updated_at

    FROM labels
      LEFT JOIN projects
        ON labels.project_id = projects.project_id

)

SELECT * FROM joined