{% set sensitive_fields = ['file_name'] %}

WITH snippets AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_snippets')}}

)

, projects AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_projects')}}

)

, namespaces AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespaces')}}

)

, namespace_lineage AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}

)
, joined AS (

    SELECT
      snippets.snippet_id,
      snippets.author_id,
      snippets.project_id,
      snippets.created_at,
      snippets.updated_at,
      snippets.snippet_type,
      snippets.visibility_level,
      {% for field in sensitive_fields %}
      CASE
        WHEN projects.visibility_level != 'public' AND NOT namespace_lineage.namespace_is_internal
          THEN 'project is private/internal'
        ELSE {{field}}
      END AS {{field}},
      {% endfor %}
      SPLIT_PART(file_name, '.', -1) AS file_extension
    FROM snippets
      LEFT JOIN projects
        ON snippets.project_id = projects.project_id
      LEFT JOIN namespaces
        ON projects.namespace_id = namespaces.namespace_id
      LEFT JOIN namespace_lineage
        ON namespaces.namespace_id = namespace_lineage.namespace_id

)

SELECT *
FROM joined
