WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.board_labels

), renamed AS (

    SELECT
      id :: integer        as board_label_relation_id,
      board_id :: integer  as board_id,
      label_id :: integer  as label_id

    FROM source


)

SELECT *
FROM renamed