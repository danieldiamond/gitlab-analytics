WITH source AS (

    SELECT *
    FROM {{ source('engineering', 'development_team_members') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), flattened AS (

    SELECT d.value as data_by_row
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d

), renamed AS (

    SELECT
      data_by_row['country']::VARCHAR                       AS country,
      data_by_row['gitlab']::VARCHAR                        AS gitlab_handle, 
      data_by_row['gitlabId']::VARCHAR                      AS gitlab_id,
      data_by_row['isBackendMaintainer']::BOOLEAN           AS is_backend_maintainer,
      data_by_row['isBackendTraineeMaintainer']::BOOLEAN    AS is_backend_trainee_maintainer,
      data_by_row['isDatabaseMaintainer']::BOOLEAN          AS is_database_maintainer,
      data_by_row['isDatabaseTraineeMaintainer']::BOOLEAN   AS is_database_trainee_maintainer,
      data_by_row['isFrontendMaintainer']::BOOLEAN          AS is_frontend_maintainer,
      data_by_row['isFrontendTraineeMaintainer']::BOOLEAN   AS is_frontend_trainee_maintainer,
      data_by_row['isManager']::BOOLEAN                     AS is_manager,
      data_by_row['level']::VARCHAR                         AS team_member_level,
      data_by_row['locality']::VARCHAR                      AS locality,
      data_by_row['location_factor']::DOUBLE PRECISION      AS location_factor,
      data_by_row['matchName']::VARCHAR                     AS match_name,
      data_by_row['name']::VARCHAR                          AS name,
      data_by_row['section']::VARCHAR                       AS development_section,
      data_by_row['start_date']::DATE                       AS start_date,
      data_by_row['team']::VARCHAR                          AS team,
      data_by_row['technology']::VARCHAR                    AS technology_group
    FROM flattened

)
SELECT *
FROM renamed