WITH employees as (

    SELECT *
    FROM {{ ref ('bamboohr_directory') }}

), contacts AS (

    SELECT *
    FROM {{ ref ('bamboohr_emergency_contacts') }}

), contacts_aggregated AS (

    SELECT
      employee_id, 
      SUM(IFF(home_phone IS NULL AND mobile_phone IS NULL AND work_phone IS NULL,1,0)) AS missing_contact
    FROM contacts
    GROUP BY 1

), final AS (

    SELECT 
      employees.*,
      IFF(contacts_aggregated.missing_contact= 0, False, True) AS missing_contact
    FROM employees
    LEFT JOIN contacts_aggregated
      ON employees.employee_id = contacts_aggregated.employee_id

)

SELECT *
FROM final
WHERE MISSING_CONTACT = TRUE
