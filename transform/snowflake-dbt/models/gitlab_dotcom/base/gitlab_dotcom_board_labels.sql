{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'board_labels') }}

), renamed AS (

    SELECT
      id :: integer        as board_label_relation_id,
      board_id :: integer  as board_id,
      label_id :: integer  as label_id

    FROM source
    WHERE rank_in_key = 1

)


SELECT *
FROM renamed