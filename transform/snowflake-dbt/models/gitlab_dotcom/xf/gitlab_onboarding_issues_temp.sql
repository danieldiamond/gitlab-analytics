with gitlab_dotcom_issues_xf as (

    SELECT *
    FROM {{ref('gitlab_dotcom_issues_xf')}}

), filtered as (

    SELECT *
    FROM gitlab_dotcom_issues_xf
    WHERE project_id = 5097604
    AND lower(issue_title) like '%onboarding%'

)

SELECT *
FROM filtered
