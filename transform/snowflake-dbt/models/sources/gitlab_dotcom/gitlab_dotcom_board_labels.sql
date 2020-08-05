WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'board_labels') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

  SELECT
    id::NUMBER        AS board_label_relation_id,
    board_id::NUMBER  AS board_id,
    label_id::NUMBER  AS label_id

  FROM source

)


SELECT *
FROM renamed
