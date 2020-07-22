WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'offices') }}

), renamed as (

	SELECT

            --keys
            id::bigint                          AS office_id,
            organization_id::bigint             AS organization_id,
            parent_id::bigint                   AS office_parent_id,

            --info
            name::varchar                       AS office_name,
            created_at::timestamp               AS office_created_at,
            updated_at::timestamp               AS office_updated_at

	FROM source

)

SELECT *
FROM renamed
