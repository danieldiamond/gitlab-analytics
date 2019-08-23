with source as (

    SELECT *
    FROM dbt_archive.sheetload_employee_location_factor_archived

), renamed as (

    SELECT bamboo_employee_number,
            location_factor,
            CASE WHEN "valid_from"::date = '2019-07-20'::date
                THEN '2000-01-20'::date ELSE "valid_from"::date END AS valid_from,
            "valid_to"::date                                        AS valid_to
    FROM source

)

SELECT *
FROM renamed
