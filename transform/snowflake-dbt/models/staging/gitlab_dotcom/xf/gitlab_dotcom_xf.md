{% docs gitlab_dotcom_ci_minutes_ui_namespace_replication %}

This table replicates the proccess that the Gitlab UI uses to generate the CI minutes Usage Quota both for personal namespaces and top level group namespaces. The codebase logic used to build this model can be seen mapped in [this diagram](https://app.lucidchart.com/documents/view/0b8b66e6-8536-4a5d-b992-9e324581187d/0_0).

It also adds two additional columns which aren't calculated in the UI, which are `limit_based_plan` and `status_based_plan` which are independent of whether there aren't projects with `shared_runners_enabled` inside the namespaces and only take into account how many minutes have been used from the monthly quota based in the plan of the namespace.

{% enddocs %}

{% docs gitlab_dotcom_daily_usage_data_events_90 %}

This table selects all the rows from `gitlab_dotcom_usage_data_events` that have an `event_date` (date when the event happened) that is less than 90 days ago. 

{% enddocs %}

{% docs gitlab_dotcom_daily_usage_data_events %}

This table is a daily aggregated table built on top of `gitlab_dotcom_usage_data_events`  table at a namespace/project level. 

{% enddocs %}

{% docs gitlab_dotcom_elasticsearch_indexed_namespaces_current %}

Only the most recent gitlab_dotcom elasticsearch_indexed_namespaces data.  
Records that have been deleted in the source database have been filtered out.

{% enddocs %}

{% docs gitlab_dotcom_environments_xf %}

This model anonymizes three fields: `environment_name`, `slug`, `external_url` based on the visibility of the projects the environments are associated to 

{% enddocs %}

{% docs gitlab_dotcom_gitlab_user_requests %}

This model enables product managers to surface which issue has been requested by potential prospects and current customers. The final model creates a table where each row is unique tuple of a `issue_id` and a `sfdc_account_id`.

It extends the models `gitlab_dotcom_notes_linked_to_sfdc_account_id` and `gitlab_dotcom_issues_linked_to_sfdc_account_id` by joining it to SFDC account metadata through the `account_id`. We add then the following metrics:

* `total_tcv`
* `carr_total`
* `count_licensed_users`

We also join the model `gitlab_dotcom_notes_linked_to_sfdc_account_id` and `gitlab_dotcom_issues_linked_to_sfdc_account_id` to `gitlab_dotcom_issues`, `gitlab_dotcom_projects` and `gitlab_dotcom_namespaces_xf` to add more metadata about issues, projects and namespaces.

{% enddocs %}

{% docs gitlab_dotcom_gitlab_user_requests_opportunities %}

This model constrains the scope of information in `gitlab_dotcom_gitlab_user_requests` specifically to Opportunities that were linked in issues, epics, and notes. This model enables us to view how much potential and/or lost revenue is associated with feature requests.

{% enddocs %}

{% docs gitlab_dotcom_internal_notes_xf %}

This model is a subset of `gitlab_dotcom_notes` model which selects only notes coming from projects in Gitlab Namespaces.

It adds a few columns to the base `gitlab_dotcom_notes` model:

* `project_name`
* `namespace_id`
* `namespace_name`
* `namespace_type`

{% enddocs %}


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

{% docs gitlab_dotcom_groups_xf %}

This model includes all columns from the groups base model and adds the count of members and projects associated with the groups.
It also adds 2 columns based on subscription inheritance (as described [here](https://about.gitlab.com/handbook/marketing/product-marketing/enablement/dotcom-subscriptions/#common-misconceptions)):

* `groups_plan_is_paid`
* `groups_plan_id`

{% enddocs %}

{% docs gitlab_dotcom_memberships %}

This model unions together all of the other models that represent a user having (full or partial) access to a namespace, AKA "membership". 

There are 5 general ways that a user can have access to a group G:
* Be a **group member** of group G.
* Be a **group member** of G2, where G2 is a descendant (subgroup) of group G.
* Be a **project member** of P, where P is owned by G or one of G's descendants.
* Be a group member of X or a parent group of X, where X is invited to a project underneath G via [project group links](https://docs.gitlab.com/ee/user/group/#sharing-a-project-with-a-group).
* Be a group member of Y or a parent group of Y, where Y is invited to G or one of G's descendants via [group group links](https://docs.gitlab.com/ee/user/group/#sharing-a-group-with-another-group).

An example of these relationships is shown in this diagram:

<div style="width: 720px; height: 480px; margin: 10px; position: relative;"><iframe allowfullscreen frameborder="0" style="width:720px; height:480px" src="https://app.lucidchart.com/documents/embeddedchart/9f529269-3e32-4343-9713-8eb311df7258" id="WRFbB73aKeB3"></iframe></div>

Additionally, this model calculates the field `is_billable` - i.e. if a member should be counted toward the seat count for a subscription (note: this also applies to namespaces without a subscription for the convenience of determining seats in use). To determine the number of seats in use for a given namespace, a simple query such as the following will suffice: 

```
SELECT COUNT(DISTINCT user_id)
FROM analytics.gitlab_dotcom_memberships
WHERE is_billable = TRUE
  AND ultimate_parent_id = 123456
```  

{% enddocs %}

{% docs gitlab_dotcom_monthly_stage_active_users %}

For each month, this model shows for each event, the users and namespaces who perform this specific event, with 2 additional measures: the number of times this event is performed by a specific user in a specific namespace, and the number of days this event is performed (for example a specific user A has opened in a namespace 1, 15 issues in 10 days)

We don't use calendar month for this calculation but the last 28 days of the month as explained in [the KPI Definition here](https://about.gitlab.com/handbook/product/metrics/#stage-monthly-active-users-smau)

{% enddocs %}


{% docs gitlab_dotcom_events_monthly_active_users%}

For each day, this model counts the number of active users from the previous 28 days. The definiton of an active user is completing one or more audit events within the timeframe. This model includes the referenced date as part of the 28-day window. So for example, the window on January 31th would be from the start of January 4th to the end of January 31 (inclusive).  

This model includes one row for every day, but MAU for a given month will typically be reported as the MAU on the **last day of the month**.

{% enddocs %}

{% docs gitlab_dotcom_merge_request_assignment_events %}

This model contains the history of assignments, unassignments, and reassignments for merge requests within internal namespaces. From `gitlab_dotcom_internal_notes_xf`, notes of type `MergeRequest` are queried. Notes are stemmed down to referenced usernames, tokenized, and flattened so that for each event (assign, unassign, reassign) a row is created for each referenced username in the order it appears in the note. Finally, usernames are replaced with user id's by joining to `gitlab_dotcom_users`. Usernames that do not have an associated user_id (if the user was deleted or changed usernames) are not included in this model so as to not misattribute assignee changes.

{% enddocs %}


{% docs gitlab_dotcom_merge_requests_xf%}

Adds associated labels for Merge Requests when these exist.

In order to achieve that we first join issues to the `label links` relation table, and then use that to join to the labels
table.

The labels are filtered in a CTE to only include `target_type = MergeRequest` as the labels table contains both Issue and Merge Request information and misattribution can happen.

In order to also add Metrics data for a Merge Request, we want to get only the last available record from the `gitlab_dotcom_merge_request_metrics` table.   
First a CTE will get the ID of the latest Merge Request Metrics snapshot, then in the following CTE we inner join to that in order to ensure we only get the latest data.

We also need to know if a MR is related to our community contributor project, there are two conditions to know if this is true:

* The label for the MR needs to be set to `community contribution`
* the namespace for the target project of the MR needs to be Gitlab.org (namespace_id = 9970)

In order to achieve this we will build a CTE from the project table that contains only project from the Gitlab.org space, then we will use this as a logical condition in a case statement.

Information about the merge request's namespace (`namespace_id`) and ultimate parent namespace (`ultimate_parent_id`, `namespace_is_internal`) is found through the **project** that the merge request is associated with. 

{% enddocs %}


{% docs gitlab_dotcom_namespaces_xf %}

Includes all columns from the namespaces base model.  
The plan columns here (plan_id, plan_title, plan_is_paid) reference the plan that is inheritted from the namespace's ultimate parent.
Adds the count of members and projects associated with the namespace.  
Also adds boolean column `namespaces_plan_is_paid` to provide extra context.  

{% enddocs %}


{% docs gitlab_dotcom_projects_xf %}

Includes all columns from the projects base model.
Adds the count of members associated with the project.
Adds a boolean column, `namespaces_plan_is_paid`, to provide extra context.
Adds additional information about the associated namespace (name and path).

{% enddocs %}


{% docs gitlab_dotcom_retention_cohorts%}

This table produces monthly retention rates by monthly signup cohort.

The `cohorting` CTE establishes how long the user was active by comparing `created_at` with `last_activity_on` and marking this length of activity in months.

The final result is determined by merging the `cohorting` table to itself when activity length = 0 so that we have the based size of the cohort, then take the rate from members active in each period of activity.

{% enddocs %}


{% docs gitlab_dotcom_saml_providers_xf %}

XF model that joins the base model `gitlab_dotcom_saml_providers` to the `gitlab_dotcom_identities` model in order to get additional metrics:

* number of users using a specific saml_provider for a specific group 
* first time a user of a specific group started using the saml_provider. This could be `NULL` if no one is using it.

{% enddocs %}

{% docs gitlab_dotcom_secure_stage_ci_jobs %}

This table is meant to isolate all ci_build jobs used to create the AMAU calculation for secure stage as described in [this handbook page](https://about.gitlab.com/handbook/product/metrics/#stage-monthly-active-users-smau)

{% enddocs %}

{% docs gitlab_dotcom_usage_data_events %}

This table produces an event table at a namespace/project level. The goal is to be able to reproduce the same usage dataset as the one sent weekly by self-managed instances to the version app.

The table normalizes all the gitlab_dotcom tables to always extract the same subset of column:
* namespace_id, 
* namespace_created_at,
* project_created_at,
* event_created_at

Currently, the following tables are included in the model:

* gitlab_dotcom_boards
* gitlab_dotcom_ci_builds
* gitlab_dotcom_ci_pipeline_schedules
* gitlab_dotcom_ci_pipelines
* gitlab_dotcom_ci_stages
* gitlab_dotcom_ci_triggers
* gitlab_dotcom_deployments
* gitlab_dotcom_environments
* gitlab_dotcom_issues
* gitlab_dotcom_labels
* gitlab_dotcom_lfs_objects_projects
* gitlab_dotcom_merge_requests
* gitlab_dotcom_milestones
* gitlab_dotcom_notes
* gitlab_dotcom_project_auto_devops
* gitlab_dotcom_releases
* gitlab_dotcom_snippets
* gitlab_dotcom_todos

A `stage_name` column is also added leveraging the seeded csv file `version_usage_stats_to_stage_mappings.csv`. When the event_name is not in the csv file, we add a condition to the case statement.

{% enddocs %}

{% docs gitlab_dotcom_users_xf%}
This model extends the base model `gitlab_dotcom_users` and adds several other dimensions

### Age cohorts
This model adds account age cohorts to the users table, the defined cohorts are:

1-  1 day or less  
2-  2 to 7 days  
3-  8 to 14 days  
4-  15 to 30 days  
5-  31 to 60 days  
6-  Over 60 days  

The CTE does this by comparing the time of the dbt run with `created_at` in the users table.

### Highest inherited subscription

This model documents the highest subscription a user inherits from. Rules around inheritance are a bit complicated, as stated in the handbook [here](https://about.gitlab.com/handbook/marketing/product-marketing/enablement/dotcom-subscriptions/#common-misconceptions),

>>>
Reality: GitLab.com subscriptions are scoped to a namespace, and individual users could participate in many groups with different subscription types. For example, they might have personal projects on a Free subscription type, participate in an open-source project that has Gold features (because it's public) while their company has a Silver subscription.
>>>

A user inherits from a subscription when:
* They are a member of a group/sub-group that has a paid subscription.
* They are a member of a project which belongs to a group with a paid subscription
* They have a personal subscription attached to their personal namespace.

Some gotchas:
* If a user is part of a public open-source (or edu) group/project, they will not inherit from the Gold subscription of the group/project.
* If a user is part of a project created by another user's personal namespace, they won't inherit from the owner's namespace subscription.

We then know for each user: what's the highest plan they inherit from and where they inherit it from.

If a user inherits from 2+ subscriptions with the same plan, we choose one subscription over the other based on the inheritance source: First, user, then groups, then projects.

### Subscription Portal (customers.gitlab.com) data 

This model surfaces also if a user has created an account or not in the subscription portal by joining with the `customers_db_customers` table. It also informs us if a specific user has already started a trial and if so when. 

### Misc

A `days_active` column is added by comparing `created_at` with `last_activity_on`

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

{% docs namespace_plan_id_at_creation %}

This column represents the gitlab_subscription plan_id (2, 3, 4, 34 or 'trial') of the namespace at the time that the object (issue, project, merge request) was created. 

{% enddocs %}
