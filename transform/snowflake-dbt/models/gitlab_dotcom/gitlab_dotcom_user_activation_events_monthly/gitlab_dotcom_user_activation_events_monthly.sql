WITH unioned AS (

    {{ dbt_utils.union_tables(tables=[ ref('gitlab_dotcom_user_issue_created_monthly'),
                                       ref('gitlab_dotcom_user_project_created_monthly'),
                                       ref('gitlab_dotcom_user_merge_request_opened_monthly')
                                     ]
                              )
    }}
)

SELECT *
FROM unioned
