WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'job_info') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

      SELECT d.value AS data_by_row
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

    SELECT 
      data_by_row['id']::NUMBER              AS job_id,
      data_by_row['employeeId']::NUMBER      AS employee_id,
      data_by_row['jobTitle']::VARCHAR       AS job_title,
      data_by_row['date']::DATE              AS effective_date,
      data_by_row['department']::VARCHAR     AS department,
      data_by_row['division']::VARCHAR       AS division,
      data_by_row['location']::VARCHAR       AS entity,
      data_by_row['reportsTo']::VARCHAR      AS reports_to
    FROM intermediate

), sheetload_job_roles AS (

    SELECT *
    FROM {{ source('sheetload', 'job_roles_prior_to_2020_02') }}

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
           ELSE NULLIF(department, '') END                                      AS department,
      CASE WHEN department = 'Meltano' THEN 'Meltano'
           WHEN division = 'Employee' THEN null
           WHEN division = 'Contractor ' THEN null
           WHEN division = 'Alliances' Then 'Sales'
           WHEN division = 'Customer Support' THEN 'Engineering'
           WHEN division = 'Customer Service' THEN 'Sales'
           ELSE NULLIF(division, '') END AS division,
      entity,
      reports_to,
      (LAG(DATEADD('day',-1,renamed.effective_date), 1) OVER (PARTITION BY renamed.employee_id ORDER BY renamed.effective_date DESC, job_id DESC)) AS effective_end_date
    FROM renamed
    
)

SELECT 
  final.*,
  sheetload_job_roles.job_role --- This will only appear for records prior to 2020-02-28 -- after this data populates in bamboohr_job_role
FROM final
LEFT JOIN sheetload_job_roles
  ON sheetload_job_roles.job_title = final.job_title
