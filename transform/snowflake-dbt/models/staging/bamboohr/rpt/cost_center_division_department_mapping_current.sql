WITH source AS (

    SELECT *
    FROM {{ ref ('employee_directory_intermediate') }}

)

    SELECT DISTINCT
    cost_center,
    division,
    department
    FROM source
    WHERE date_actual = CURRENT_DATE()
    AND is_termination_date = False 