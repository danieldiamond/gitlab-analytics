{% set fields_to_mask = ['title', 'description'] %}



WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.issues

),
private_projects AS (

  SELECT
     project_id,
    'not-public' as visibility
  FROM {{ ref('gitlab_dotcom_projects') }}
  WHERE visibility_level != 'public'

), renamed AS (

    SELECT

      id :: integer                                               as issue_id,
      author_id :: integer                                        as author_id,
      source.project_id :: integer                                as project_id,
      TRY_CAST(milestone_id as integer)                           as milestone_id,
      TRY_CAST(updated_by_id as integer)                          as updated_by_id,
      TRY_CAST(last_edited_by_id as integer)                      as last_edited_by_id,
      TRY_CAST(moved_to_id as integer)                            as moved_to_id,
      created_at :: timestamp                                     as issue_created_at,
      updated_at :: timestamp                                     as issue_updated_at,
      TRY_CAST(last_edited_at as timestamp)                       as last_edited_at,
      TRY_CAST(closed_at as timestamp)                            as closed_at,
      confidential :: boolean                                     as is_confidential,

      {% for field in fields_to_mask %}
      CASE
        WHEN LOWER(confidential) = 'true' THEN 'confidential issue - content masked'
        WHEN private_projects.visibility = 'not-public' THEN 'private/internal project - content masked'
        ELSE {{field}}
      END                                                         as issue_{{field}},
      {% endfor %}

      state,
      TRY_CAST(weight as number)                                  as weight,
      TRY_CAST(due_date as date)                                  as due_date,
      TRY_CAST(lock_version as number)                            as lock_version,
      TRY_CAST(time_estimate as number)                           as time_estimate,
      TRY_CAST(discussion_locked as boolean)                      as has_discussion_locked,
      TO_TIMESTAMP(_updated_at :: integer)                        as issues_last_updated_at

    FROM source left join
      private_projects on source.project_id = private_projects.project_id


)

SELECT *
FROM renamed