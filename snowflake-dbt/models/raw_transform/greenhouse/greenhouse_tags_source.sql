WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'tags') }}

), renamed as (

	SELECT

                --keys
                id::bigint                  AS tag_id,
                organization_id::bigint     AS organization_id,

                --info
                name::varchar               AS tag_name,
                created_at::timestamp       AS tag_created_at,
                updated_at::timestamp       AS tag_updated_at

	FROM source

)

SELECT *
FROM renamed
