WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'services') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                            AS service_id,
      type::VARCHAR                          AS service_type,
      title::VARCHAR                         AS service_title,
      project_id::INTEGER                    AS project_id,
      created_at::TIMESTAMP                  AS created_at,
      updated_at::TIMESTAMP                  AS updated_at,
      active::BOOLEAN                        AS is_active,
      properties::VARCHAR                    AS service_properties,
      template::BOOLEAN                      AS service_template,
      push_events::BOOLEAN                   AS has_push_events,
      issues_events::BOOLEAN                 AS has_issues_events,
      merge_requests_events::BOOLEAN         AS has_merge_requests_events,
      tag_push_events::BOOLEAN               AS has_tag_push_events,
      note_events::BOOLEAN                   AS has_note_events,
      category::VARCHAR                      AS service_catetgory,
      wiki_page_events::BOOLEAN              AS has_wiki_page_events,
      pipeline_events::BOOLEAN               AS has_pipeline_events,
      confidential_issues_events::BOOLEAN    AS has_confidential_issues_events,
      commit_events::BOOLEAN                 AS has_commit_events,
      job_events::BOOLEAN                    AS has_job_events,
      confidential_note_events::BOOLEAN      AS has_confidential_note_events,
      deployment_events::BOOLEAN             AS has_deployment_events,
      comment_on_event_enabled::BOOLEAN      AS is_comment_on_event_enabled
    FROM source

)

SELECT *
FROM renamed
