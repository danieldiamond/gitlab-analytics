{{ config({
		"schema": "staging"
		})
}}

WITH source AS (

	SELECT
		*
	FROM {{ source('gitlab_dotcom', 'resource_label_events') }}

), deduplicated AS (
	
	SELECT
	  *,
		{{ dbt_utils.surrogate_key('action', 'issue_id', 
															   'merge_request_id', 'epic_id', 
																 'label_id', 'user_id', 'created_at') }}    AS surrogate_key_id,
		ROW_NUMBER() OVER (PARTITION BY surrogate_key_id ORDER BY _uploaded_at) AS rank_in_key
		
	FROM source
)

, renamed AS (

	SELECT
		action::INTEGER                  AS action_type_id,
		issue_id::INTEGER                AS issue_id,
		merge_request_id::INTEGER        AS merge_request_id,
		epic_id::INTEGER                 AS epic_id,
		label_id::INTEGER                AS label_id,
		user_id::INTEGER                 AS user_id,
		created_at::TIMESTAMP            AS created_at,
		cached_markdown_version::VARCHAR AS cached_markdown_version,
		reference::VARCHAR               AS referrence,
		reference_html::VARCHAR          AS reference_html
		
	FROM deduplicated
	WHERE rank_in_key = 1

)

SELECT *
FROM renamed
