WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'departments') }}

), renamed AS (

    SELECT department_id::float       AS department_id,
           name::varchar              AS department_name,
           full_name::varchar         AS department_full_name,
           parent_id::float           AS parent_department_id,
           isinactive::boolean        AS is_department_inactive

    FROM source

)

SELECT *
FROM renamed
