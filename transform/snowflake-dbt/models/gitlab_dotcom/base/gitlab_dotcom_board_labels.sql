WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'board_labels') }}

), renamed AS (

    SELECT
      id::INTEGER        AS board_label_relation_id,
      board_id::INTEGER  AS board_id,
      label_id::INTEGER  AS label_id

    FROM source
    WHERE rank_in_key = 1

)


SELECT *
FROM renamed
