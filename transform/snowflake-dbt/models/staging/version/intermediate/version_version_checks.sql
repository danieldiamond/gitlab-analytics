{{ config({
    "materialized": "table"
    })
}}

WITH base AS (

    SELECT *
    FROM {{ ref('version_version_checks_source') }}

), final AS (

    SELECT
      id,
      host_id,
      created_at,
      updated_at,
      gitlab_version,
      referer_url,
      request_data['HTTP_USER_AGENT']::VARCHAR      AS http_user_agent,
      request_data['HTTP_REFERER']::VARCHAR         AS http_referer,
      request_data['HTTP_ACCEPT_LANGUAGE']::VARCHAR AS http_accept_language,
      request_data
    FROM base  

)

SELECT *
FROM final