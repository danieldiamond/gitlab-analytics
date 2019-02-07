WITH source AS (

	SELECT *
	FROM raw.gitlab_dotcom.label_links

), renamed AS (

    SELECT

      id :: integer                                    as label_link_id,
      label_id :: integer                              as label_id,
      target_id :: integer                             as target_id,
      target_type,
      created_at :: timestamp                          as label_link_created_at,
      updated_at :: timestamp                          as label_link_updated_at


    FROM source


)

SELECT *
FROM renamed