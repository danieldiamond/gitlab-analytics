{{ config({
    "materialized": "incremental",
    "unique_key": "note_id"
    })
}}


{% set fields_to_mask = ['note'] %}

WITH base AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_notes') }}
    {% if is_incremental() %}

      WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}
)

, projects AS (

    SELECT * 
    FROM {{ ref('gitlab_dotcom_projects_xf') }}
)

, internal_namespaces AS (
  
    SELECT 
      namespace_id,
      namespace_ultimate_parent_id,
      (namespace_ultimate_parent_id IN {{ get_internal_parent_namespaces() }}) AS namespace_is_internal
    FROM {{ ref('gitlab_dotcom_namespaces_xf') }}

)

, anonymised AS (
    
    SELECT
      {{ dbt_utils.star(from=ref('gitlab_dotcom_notes'), except=fields_to_mask|upper, relation_alias='base') }},
      {% for field in fields_to_mask %}
        CASE
          WHEN TRUE 
            AND projects.visibility_level != 'public'
            AND NOT internal_namespaces.namespace_is_internal
            THEN 'confidential - masked'
          ELSE {{field}}
        END AS {{field}},
      {% endfor %}
      projects.ultimate_parent_id
    FROM base
      LEFT JOIN projects 
        ON base.project_id = projects.project_id
      LEFT JOIN internal_namespaces
        ON projects.namespace_id = internal_namespaces.namespace_id

)

SELECT * 
FROM anonymised
