{% docs gitlab_dotcom_user_activation_events_monthly%}

This model provides a summary of several event tables unioned together at the granularity of one row per user per month per event type.

At the moment the following events are part of this table:

* `project_created`
* `issue_created`
* `merge_request_opened`

In months where a user does not record a specific event, a row will still be created with a count of zero.

This model provides `user_created_at_month` and `months_since_join_date` columns to allow for easy cohort and retention analysis.

{% enddocs %}

{% docs gitlab_dotcom_user_issue_created_monthly%}

This model provides a summary of the `gitlab_dotcom_issues` table at the granularity of one row per user per month.

In months where a user does not record a specific event, a row will still be created with a count of zero.

This model provides `user_created_at_month` and `months_since_join_date` columns to allow for easy cohort and retention analysis.

This model is used in the `gitlab_dotcom_user_activation_events_monthly` model that will be used as the foundational model of cohort analysis for the activation team.

{% enddocs %}

{% docs gitlab_dotcom_user_merge_request_opened_monthly%}

This model provides a summary of the `gitlab_dotcom_merge_requests` table at the granularity of one row per user per month.

In months where a user does not record a specific event, a row will still be created with a count of zero.

This model provides `user_created_at_month` and `months_since_join_date` columns to allow for easy cohort and retention analysis.

This model is used in the `gitlab_dotcom_user_activation_events_monthly` model that will be used as the foundational model of cohort analysis for the activation team.

{% enddocs %}

{% docs gitlab_dotcom_user_project_created_monthly%}

This model provides a summary of the `gitlab_dotcom_projects` table at the granularity of one row per user per month.

In months where a user does not record a specific event, a row will still be created with a count of zero.

This model provides `user_created_at_month` and `months_since_join_date` columns to allow for easy cohort and retention analysis.

This model is used in the `gitlab_dotcom_user_activation_events_monthly` model that will be used as the foundational model of cohort analysis for the activation team.

{% enddocs %}
