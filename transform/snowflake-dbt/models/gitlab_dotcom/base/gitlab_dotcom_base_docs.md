{% docs gitlab_dotcom_groups %}

This is the base model for Gitlab.com groups. It is a subset of the namespaces table which includes both individual namespaces (when a user is created, a personal namespace is created) and groups (and subgroups) which is a collaborative namespace where several users can collaborate on specific projects.

{% enddocs %}


{% docs gitlab_dotcom_namespace_lineage %}

This model has one row for each namespace in the namespaces base model. This model adds extra information about all of the upstream parents associated with the namespace.  

The `upstream_lineage` column is an array with the namespaces's entire geneology, ordered from young to old (self, parent, grandparent).  

Since groups can be nested up to 21 levels deep, this model provides an `ultimate_parent_id` column which does the work of finding the top-level namespace for each namespace, using a recusive CTE.  This column is always the same as the last (furthest right) item of `upstream_lineage`.  

The recurvice CTE uses a top-down approach to iterate though each namespace. The anchor section selects all namespaces without parents. The iteration section recursively joins through all children onto the anchor wherever anchor.namespace == iteration.parent_namespace.  

{% enddocs %}


{% docs gitlab_dotcom_gitlab_subscriptions %}

Base model for Gitlab.com gitlab_subscriptions. These are the plan subscriptions for the GitLab.com product, as opposed to the `subscriptions` table (no prefix) which deals with subscriptions in the notification sense, related to issues and merge requests.

Note: the primary_key in this model is `namespace_id`.

{% enddocs %}


{% docs scd_type_two_documentation %}
<br/>
### Type 2 Slowly Changing Dimension
This base model is modelled as a Type 2 Slowly Changing Dimension. This means 3 columns have been added as metadata to track row-level changes over time. These columns are `valid_from`, `valid_to` and `is_currently_valid`. One implication of this is that the primary key column in this table is *not* unique. There can be multiple rows per primary_key, but only a maximum of one will have `is_currently_vaild` set to TRUE. 

Read the documentation for the SCD Type 2 Macro [here](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/README.md#scd_type_2).

{% enddocs %}


{% docs gitlab_dotcom_events %}

Base model for Gitlab.com events. Events are documented [here](https://docs.gitlab.com/ee/api/events.html).
We do one transformation in this base model where we map an `action_type_id` to an `action_type` thanks to the macro `action_type`.

{% enddocs %}


{% docs visibility_documentation %}
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

{% docs gitlab_dotcom_resource_label_events %}

Base model for Gitlab.com resource label events. Explanation [here](https://docs.gitlab.com/ee/api/resource_label_events.html). 

We map the `action_type_id` column to an `action_type` using the macro `resource_label_action_type`

{% enddocs %}
