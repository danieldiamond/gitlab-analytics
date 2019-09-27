WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'departments') }}

), renamed AS (

    SELECT department_id,
           name                AS department_name,
           full_name           AS department_full_name,
           parent_id           AS parent_department_id,
           isinactive::boolean AS is_department_inactive

    FROM source

)

SELECT *
FROM renamed
