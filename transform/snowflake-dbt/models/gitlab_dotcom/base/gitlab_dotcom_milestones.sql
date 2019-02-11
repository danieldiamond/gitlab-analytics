WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.milestones

), renamed AS (

    SELECT

      id :: integer                           as milestone_id,

      TRY_CAST(project_id as integer)         as project_id,
      TRY_CAST(group_id as integer)           as group_id,

      TRY_CAST(start_date as date)            as start_date,
      TRY_CAST(due_date as date)              as due_date,
      state                                   as milestone_status,

      created_at :: timestamp                 as milestone_created_at,
      updated_at :: timestamp                 as milestone_updated_at

    FROM source


)

SELECT *
FROM renamed