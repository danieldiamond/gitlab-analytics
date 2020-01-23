WITH source AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'services') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::INTEGER                            AS service_id
      type::VARCHAR                          AS
      title::VARCHAR                         AS
      project_id::INTEGER                    AS
      created_at::TIMESTAMP                  AS
      updated_at::TIMESTAMP                  AS
      active::BOOLEAN                        AS
      properties::VARCHAR                    AS
      template::BOOLEAN                      AS
      push_events::BOOLEAN                   AS
      issues_events::BOOLEAN                 AS
      merge_requests_events::BOOLEAN         AS
      tag_push_events::BOOLEAN               AS
      note_events::BOOLEAN                   AS
      category::VARCHAR                      AS
      wiki_page_events::BOOLEAN              AS
      pipeline_events::BOOLEAN               AS
      confidential_issues_events::BOOLEAN    AS
      commit_events::BOOLEAN                 AS
      job_events::BOOLEAN                    AS
      confidential_note_events::BOOLEAN      AS
      deployment_events::BOOLEAN             AS
      comment_on_event_enabled::BOOLEAN      AS
    FROM source

)

SELECT *
FROM renamed
