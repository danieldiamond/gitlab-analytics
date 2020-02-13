WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'users') }}

), renamed as (

	SELECT
            --keys
            id::bigint                  AS user_id,
            organization_id::bigint     AS organization_id,
            employee_id::varchar        AS employee_id,

            --info
            status::varchar             AS user_status,
            created_at::timestamp       AS user_created_at,
            updated_at::timestamp       AS user_updated_at

	FROM source

)

SELECT *
FROM renamed
