WITH source as (

    SELECT *

      FROM {{ source('greenhouse', 'departments') }}

), renamed as (

    SELECT

                --keys
                id::bigint                  AS department_id,
                organization_id::bigint     AS organization_id,
                parent_id::bigint           AS parent_id,

                --info
                name::varchar               AS department_name,
                created_at::timestamp       AS department_created_at,
                updated_at::timestamp       AS department_updated_at


    FROM source

)

SELECT *
FROM renamed
