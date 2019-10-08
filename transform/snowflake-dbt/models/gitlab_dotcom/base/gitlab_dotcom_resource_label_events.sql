{{ config({
		"schema": "staging"
		})
}}

WITH source AS (

	SELECT
		*
	FROM {{ source('gitlab_dotcom', 'resource_label_events') }}

), renamed AS (

	SELECT
		action::INTEGER                  AS action_id,
		issue_id::INTEGER                AS issue_id,
		merge_request_id::INTEGER        AS merge_request_id,
		epic_id::INTEGER                 AS epic_id,
		label_id::INTEGER                AS label_id,
		user_id::INTEGER                 AS user_id,
		created_at::TIMESTAMP            AS created_at,
		cached_markdown_version::VARCHAR AS cached_markdown_version,
		reference::VARCHAR               AS referrence,
		reference_html::VARCHAR          AS reference_html
		
	FROM source

)

SELECT *
FROM renamed
