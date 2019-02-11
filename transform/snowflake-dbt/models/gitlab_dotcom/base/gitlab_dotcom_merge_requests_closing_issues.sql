WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.merge_requests_closing_issues

), renamed AS (

    SELECT
      DISTINCT md5(merge_request_id || issue_id || created_at)    as merge_request_issue_relation_id,
      merge_request_id :: integer                                 as merge_request_id,
      issue_id :: integer                                         as issue_id,
      created_at :: timestamp                                     as merge_request_closing_issue_created_at,
      updated_at :: timestamp                                     as merge_request_closing_issue_updated_at

    FROM source


)

SELECT *
FROM renamed
