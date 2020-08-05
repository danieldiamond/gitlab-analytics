WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'scorecards_attributes') }}

), renamed as (

	SELECT

            --keys
            scorecard_id::bigint                AS scorecard_id,
            attribute_id::bigint                AS scorecard_attribute_id,

            --info
            rating::varchar                     AS scorecard_attribute_rating,
            notes::varchar                      AS scorecard_attribute_notes,
            created_at::timestamp               AS scorecard_attribute_created_at,
            updated_at::timestamp               AS scorecard_attribute_updated_at


	FROM source

)

SELECT *
FROM renamed
