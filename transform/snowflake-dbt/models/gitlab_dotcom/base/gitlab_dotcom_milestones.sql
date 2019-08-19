{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'milestones') }}

), renamed AS (

    SELECT

      id :: integer                           as milestone_id,
      project_id::integer                     as project_id,
      group_id::integer                       as group_id,
      start_date::date                        as start_date,
      due_date::date                          as due_date,
      state                                   as milestone_status,

      created_at :: timestamp                 as milestone_created_at,
      updated_at :: timestamp                 as milestone_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
