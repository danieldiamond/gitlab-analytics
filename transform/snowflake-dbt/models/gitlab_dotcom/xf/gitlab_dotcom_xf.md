{% docs gitlab_dotcom_environments_xf %}

This model anonymizes three fields: `environment_name`, `slug`, `external_url` based on the visibility of the projects the environments are associated to 

{% enddocs %}

{% docs gitlab_dotcom_gitlab_issues_requests %}

This model enables product managers to surface which issue has been requested by potential prospects and current customers. The final model creates a table where each row is unique tuple of a `issue_id` and a `sfdc_account_id`.

It extends the models `gitlab_dotcom_notes_linked_to_sfdc_account_id` and `gitlab_dotcom_issues_linked_to_sfdc_account_id` by joining it to SFDC account metadata through the `account_id`. We add then the following metrics:

* `total_tcv`
* `carr_total`
* `count_licensed_users`

We also join the model `gitlab_dotcom_notes_linked_to_sfdc_account_id` and `gitlab_dotcom_issues_linked_to_sfdc_account_id` to `gitlab_dotcom_issues`, `gitlab_dotcom_projects` and `gitlab_dotcom_namespaces_xf` to add more metadata about issues, projects and namespaces.

{% enddocs %}

{% docs gitlab_dotcom_internal_notes_xf %}

This model is a subset of `gitlab_dotcom_notes` model which selects only notes coming from projects in Gitlab Namespaces.

It adds a few columns to the base `gitlab_dotcom_notes` model:

* `project_name`
* `namespace_id`
* `namespace_name`
* `namespace_type`

{% enddocs %}

{% docs gitlab_dotcom_group_audit_events_monthly%}

This model provides a summary of the audit_events table at the granularity of one row per group per month.

In months where a group does not record any audit events, a row will still be created with a count of zero.

Audit events are often used as a proxy for user activity, so this model allows for convenient Monthly Active Group calculations without having to query the entire audit_events table.

This model provides `group_created_at_month` and `months_since_creation_date` columns to allow for easy cohort and retention analysis.

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


{% docs gitlab_dotcom_events_monthly_active_users%}

For each day, this model counts the number of active users from the previous 28 days. The definiton of an active user is completing one or more audit events within the timeframe. This model includes the referenced date as part of the 28-day window. So for example, the window on January 31th would be from the start of January 4th to the end of January 31 (inclusive).  

This model includes one row for every day, but MAU for a given month will typically be reported as the MAU on the **last day of the month**.

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

{% enddocs %}


{% docs gitlab_dotcom_namespaces_xf %}

Includes all columns from the namespaces base model. Note that `namespaces.plan_id` is overridden by the `plan_id` from the `gitlab_subscriptions` model.
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

{% docs gitlab_dotcom_user_audit_events_monthly%}

This model provides a summary of the audit_events table at the granularity of one row per user per month.

In months where a user does not record any audit events, a row will still be created with a count of zero.

Audit events are often used as a proxy for user activity, so this model allows for convenient MAU calculations without having to query the entire audit_events table.

This model provides `user_created_at_month` and `months_since_join_date` columns to allow for easy cohort and retention analysis. One difference between this model and the `gitlab_dotcom_retention_cohorts` model is that this model knows about activity for every single month, whereas the latter only uses the `created_at` and `last_activity_on` user timestamps and is forced to fill in the gaps for the months in the middle.

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
