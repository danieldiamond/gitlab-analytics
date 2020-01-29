WITH members AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_members')}}

), project_group_links AS (

  SELECT *
  FROM {{ref('gitlab_dotcom_members')}}

)

project_group_links_unnested AS (

  
)
