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