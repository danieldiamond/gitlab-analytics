WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'approvals') }}

), renamed AS (

    SELECT
      id :: integer                     as approval_id,
      merge_request_id :: integer       as merge_request_id,
      user_id :: integer                as user_id,
      created_at :: timestamp           as approval_created_at,
      updated_at :: timestamp           as approval_updated_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
