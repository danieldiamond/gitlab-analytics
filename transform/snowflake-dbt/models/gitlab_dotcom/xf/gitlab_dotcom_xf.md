{% docs gitlab_dotcom_issues_xf %}

Adds associated labels for issues when these exist. 

In order to achieve that we first join issues to the `label links` relation table, and then use that to join to the labels table.


This transformation also masks title/description based on privacy of the project that it is on and the confidentiality setting on the issue.  

A CTE will find projects that don't have visibility set to public and then joined to the issues in order to build a CASE statement to mask the content.



{% enddocs %}


{% docs gitlab_dotcom_labels_xf %}

Masks the label description based on privacy of the project that it is on. 

A CTE will find projects that don't have visibility set to public and then joined to the labels in order to build a CASE statement to mask the content.

{% enddocs %}


{% docs gitlab_dotcom_merge_requests_xf%}

Adds associated labels for Merge Requests when these exist. 

In order to achieve that we first join issues to the `label links` relation table, and then use that to join to the labels 
table.

The labels are filtered in a CTE to only include `target_type = MergeRequest` as the labels table contains both Issue and Merge Request information and missatribution can happen. 

In order to also add Metrics data for a Merge Request, we want to get only the last available record from the `gitlab_dotcom_merge_request_metrics` table.   
First a CTE will get the ID of the latest Merge Request Metrics snapshot, then in the following CTE we inner join to that in order to ensure we only get the latest data. 

We also need to know if a MR is related to our community contributor project, there are two conditions to know if this is true:

* The label for the MR needs to be set to `community contribution`
* the namespace for the target project of the MR needs to be Gitlab.org (namespace_id = 9970)

In order to achieve this we will build a CTE from the project table that contains only project from the Gitlab.org space, then we will use this as a logical condition in a case statement. 

{% enddocs %}


{% docs gitlab_dotcom_user_audit_events_monthly%}

This model provides a summary of the audit_events table at the granularity of one row per user per month.

In months where a user does not record any audit events, a row will still be created with a count of zero.

Audit events are often used as a proxy for user activity, so this model allows for convenient MAU calculations without having to query the entire audit_events table. 

This model provides `user_created_at_month` and `months_since_join_date` columns to allow for easy cohort and retention analysis. One difference between this model and the `gitlab_dotcom_retention_cohorts` model is that this model knows about activity for every single month, whereas the latter only uses the `created_at` and `last_activity_on` user timestamps and is forced to fill in the gaps for the months in the middle.

{% enddocs %}


{% docs gitlab_dotcom_users_xf%}
Adds account age cohorts to the users table, the defined cohorts are:

1-  1 day or less  
2-  2 to 7 days  
3-  8 to 14 days  
4-  15 to 30 days  
5-  31 to 60 days  
6-  Over 60 days  

The CTE does this by comparing the time of the dbt run with `created_at` in the users table. 

Also a definition is made for account activity time, by comparing `created_at` with `last_activity_on`

{% enddocs %}

{% docs gitlab_dotcom_retention_cohorts%}

This table produces monthly retention rates by monthly signup cohort. 

The `cohorting` CTE establishes how long the user was active by comparing `created_at` with `last_activity_on` and marking this length of activity in months. 

The final result is determined by merging the `cohorting` table to itself when activity length = 0 so that we have the based size of the cohort, then take the rate from members active in each period of activity.



{% enddocs %}

{% docs xf_visibility_documentation %}
This content will be masked for privacy in one of the following conditions:
 * If this is an issue, and the issue is set to `confidential`
 * If the namespace or project visibility level is set to "internal" (`visibility_level` = 10) or "private" (`visibility_level` = 0).
    * The visibility values can be validated by going to the [project navigation](https://gitlab.com/explore) and using the keyboard shortcut "pb" to show how the front-end queries for visibility.
 * Public projects are defined with a `visibility_level` of 20   
 * In all the above cases,  the content will *not* be masked if the namespace_id is in: 
   * 6543: gitlab-com
   * 9970: gitlab-org
   * 4347861: gitlab-data  
     
{% enddocs %}