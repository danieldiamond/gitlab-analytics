{{
  config({
    "materialized": "incremental",
    "unique_key": "ci_build_id"
  })
}}

{{ dbt_utils.union_relations(
    relations=[
                ref('gitlab_dotcom_container_scanning_jobs'), 
                ref('gitlab_dotcom_dast_jobs'), 
                ref('gitlab_dotcom_dependency_scanning_jobs'), 
                ref('gitlab_dotcom_license_management_jobs'), 
                ref('gitlab_dotcom_license_scanning_jobs'), 
                ref('gitlab_dotcom_sast_jobs')
              ]
) }}
