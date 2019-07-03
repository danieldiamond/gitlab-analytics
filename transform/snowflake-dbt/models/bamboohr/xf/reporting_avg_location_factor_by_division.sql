{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

with employee_directory AS(

    SELECT * FROM {{ref('employee_directory')}}

), modeled AS (

    SELECT date_trunc('month', hire_date)::date                                         AS start_month,
            division,
            AVG(location_factor)  OVER (PARTITION BY (division) ORDER BY start_month)   AS average_location_factor,
            SUM(distinct coalesce(coalesce(first_name, ' '), last_name)) OVER (PARTITION BY (division) ORDER BY start_month) AS headcount
    FROM employee_directory
    WHERE hire_date IS NOT NULL
    ORDER BY 1 DESC
)

SELECT *
FROM modeled
GROUP BY 1, 2, 3, 4
