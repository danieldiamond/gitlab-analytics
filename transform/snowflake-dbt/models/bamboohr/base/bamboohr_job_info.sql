WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'job_info') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

      SELECT d.value as data_by_row
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

      SELECT data_by_row['id']::bigint          AS job_id,
             data_by_row['employeeId']::bigint  AS employee_id,
             data_by_row['jobTitle']::varchar   AS job_title,
             data_by_row['date']::date          AS effective_date,
             data_by_row['department']::varchar AS department,
             data_by_row['division']::varchar   AS division,
             data_by_row['location']::varchar   AS entity,
             data_by_row['reportsTo']::varchar  AS reports_to
      FROM intermediate

), job_role AS (

    SELECT * 
    FROM {{ ref('bamboohr_job_role') }}

{# ), sheetload_job_roles AS (

    SELECT * 
    FROM {{ ref('sheetload_job_roles_prior_to_2020_02') }} #}

), final AS (

    SELECT 
    job_id,
    renamed.employee_id,
    job_title,
    renamed.effective_date, --the below case when statement is also used in employee_directory_analysis until we upgrade to 0.14.0 of dbt
    CASE WHEN division = 'Alliances' THEN 'Alliances'
        WHEN division = 'Customer Support' THEN 'Customer Support'
        WHEN division = 'Customer Service' THEN 'Customer Success'
        WHEN department = 'Data & Analytics' THEN 'Business Operations'
        ELSE nullif(department, '') END                                      AS department,
    CASE WHEN department = 'Meltano' THEN 'Meltano'
        WHEN division = 'Employee' THEN null
        WHEN division = 'Contractor ' THEN null
        WHEN division = 'Alliances' Then 'Sales'
        WHEN division = 'Customer Support' THEN 'Engineering'
        WHEN division = 'Customer Service' THEN 'Sales'
        ELSE nullif(division, '') END AS division,
    entity,
    reports_to,
    (LAG(DATEADD('day',-1,renamed.effective_date), 1) OVER (PARTITION BY renamed.employee_id ORDER BY renamed.effective_date DESC)) AS effective_end_date
    FROM renamed
    
)

SELECT 
  final.*,
  job_role
  --    iff(job_info.effective_date< '2020-02-28', job_role_prior_to_capture.job_role, job_role.job_role) as job_role,

FROM final
 {# LEFT JOIN sheetload_job_roles
      ON sheetload_job_roles.job_title = renamed.job_title #} 
LEFT JOIN job_role
      ON job_role.employee_id = final.employee_id
      AND job_role.effective_date BETWEEN final.effective_date and COALESCE(final.effective_end_date, {{max_date_in_bamboo_analyses()}})
