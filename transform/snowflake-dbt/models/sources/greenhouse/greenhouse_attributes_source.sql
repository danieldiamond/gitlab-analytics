WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'attributes') }}

), renamed as (

	SELECT

            --keys
            id::bigint                          AS attribute_id,
            organization_id::bigint             AS organization_id,

            --info
            name::varchar                       AS attribute_name,
            category::varchar                   AS attribute_category,
            created_at::varchar::timestamp      AS attribute_created_at,
            updated_at::varchar::timestamp      AS attribute_updated_at

	FROM source

)

SELECT *
FROM renamed
