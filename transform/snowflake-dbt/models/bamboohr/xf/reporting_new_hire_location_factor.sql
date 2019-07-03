{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

with employee_directory AS(

  SELECT * FROM {{ref('employee_directory')}}

)

SELECT  date_trunc('month', hire_date)::date                           AS start_month,
        department,
        division,
        avg(location_factor)                                           AS new_hire_location_factor,
        count(distinct coalesce(coalesce(first_name, ' '), last_name)) AS new_hires
FROM employee_directory
WHERE hire_date IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1 DESC
