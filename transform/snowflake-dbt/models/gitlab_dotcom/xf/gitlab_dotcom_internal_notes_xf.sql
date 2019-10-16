WITH notes AS (

  SELECT
    *,
    {{target.schema}}_staging.regexp_to_array(note, '(?<=(gitlab.my.|na34.)salesforce.com\/)[0-9a-zA-Z]{15,18}') AS sfdc_link_array
  FROM {{ ref('gitlab_dotcom_notes') }}

)

, projects AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_projects') }}

)

, internal_namespaces AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespaces_xf') }}
  WHERE namespace_is_internal

)

, notes_with_namespaces AS (

  SELECT
    notes.*,
    projects.project_name,
    internal_namespaces.namespace_id,
    internal_namespaces.namespace_name

  FROM notes
  LEFT JOIN projects
    ON notes.note_project_id = projects.project_id
  INNER JOIN internal_namespaces
    ON projects.namespace_id = internal_namespaces.namespace_id

)

SELECT
*
FROM notes_with_namespaces
