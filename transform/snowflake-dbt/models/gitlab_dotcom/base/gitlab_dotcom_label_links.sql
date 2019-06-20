WITH source AS (

	SELECT *,
				ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'label_links') }}

), renamed AS (

    SELECT

      id :: integer                                    as label_link_id,
      label_id :: integer                              as label_id,
      target_id :: integer                             as target_id,
      target_type,
      created_at :: timestamp                          as label_link_created_at,
      updated_at :: timestamp                          as label_link_updated_at


    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
