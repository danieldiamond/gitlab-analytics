WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_elasticsearch_indexed_namespaces_source') }}

), latest AS (

    SELECT
      namespace_id,
      created_at,
      updated_at
    FROM source
    WHERE task_instance_name = (
        SELECT
          MAX(task_instance_name)
        FROM source
    )

)

SELECT *
FROM latest