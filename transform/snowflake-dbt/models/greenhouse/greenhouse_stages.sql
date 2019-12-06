WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'stages') }}

), renamed as (

	SELECT

            --keys
            id::bigint                  AS stage_id,
            organization_id::bigint     AS organization_id,

            --info
            name::varchar             	AS stage_name,
            "order"::int                AS stage_order,
            active::boolean             AS is_active


	FROM source

)

SELECT *
FROM renamed
