{{
  config({
    "materialized": "incremental"
  })
}}

WITH ci_builds AS (
  
  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_builds') }}

), secure_ci_builds AS (
  
  SELECT 
    *,
    CASE
    WHEN ci_build_name LIKE '%container_scanning%' 
      THEN 'container_scanning'
    WHEN ci_build_name LIKE '%dast%'  
      THEN 'dast' 
    WHEN ci_build_name LIKE '%dependency_scanning%'  
      THEN 'dependency_scanning'
    WHEN ci_build_name LIKE '%license_management%'  
      THEN 'license_management'
    WHEN ci_build_name LIKE '%license_scanning%'  
      THEN 'license_scanning'
    WHEN ci_build_name LIKE '%sast%'  
      THEN 'sast'  
    END AS secure_ci_job_type
  FROM ci_builds 
  WHERE (
          ci_build_name LIKE '%container_scanning%' OR
          ci_build_name LIKE '%dast%' OR
          ci_build_name LIKE '%dependency_scanning%' OR
          ci_build_name LIKE '%license_management%' OR
          ci_build_name LIKE '%license_scanning%' OR
          ci_build_name LIKE '%sast%'
          
)

SELECT *
FROM secure_ci_builds
