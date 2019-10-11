{% set paid_plans = (2, 3, 4) %}
{% set fields_to_mask = ['namespace_name', 'namespace_path'] %}

WITH namespaces AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespaces')}}

),

members AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_members')}}

),

projects AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_projects')}}

), namespace_lineage AS (

    SELECT
      namespace_id,
      ultimate_parent_id,
      ( ultimate_parent_id IN {{ get_internal_parent_namespaces() }} ) AS namespace_is_internal
    FROM {{ref('gitlab_dotcom_namespace_lineage')}}

), gitlab_subscriptions AS (

    /*
      Here we only want the current (most recent) gitlab subscription for each namespace.
      This windowing approach does not handle hard deletes correctly. 
      We will develop a more elegant approach after https://gitlab.com/gitlab-data/analytics/issues/2727
    */
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY gitlab_subscription_updated_at DESC) AS rank_in_namespace_id
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions')}}

), joined AS (
    SELECT
      namespaces.namespace_id,
      namespace_lineage.namespace_is_internal,

      {% for field in fields_to_mask %}
      CASE
        WHEN namespaces.visibility_level = 'public' OR namespace_is_internal THEN {{field}}
        WHEN namespaces.visibility_level = 'internal' THEN 'internal - masked'
        WHEN namespaces.visibility_level = 'private'  THEN 'private - masked'
      END                                                              AS {{field}},
      {% endfor %}

      namespaces.owner_id,
      COALESCE(namespaces.namespace_type, 'Individual')                AS namespace_type,
      namespaces.has_avatar,
      namespaces.namespace_created_at,
      namespaces.namespace_updated_at,
      namespaces.is_membership_locked,
      namespaces.has_request_access_enabled,
      namespaces.has_share_with_group_locked,
      namespaces.visibility_level,
      namespaces.ldap_sync_status,
      namespaces.ldap_sync_error,
      namespaces.ldap_sync_last_update_at,
      namespaces.ldap_sync_last_successful_update_at,
      namespaces.ldap_sync_last_sync_at,
      namespaces.lfs_enabled,
      namespaces.parent_id,
      namespace_lineage.ultimate_parent_id                             AS namespace_ultimate_parent_id,
      namespaces.shared_runners_minutes_limit,
      namespaces.repository_size_limit,
      namespaces.does_require_two_factor_authentication,
      namespaces.two_factor_grace_period,
      namespaces.project_creation_level,

      gitlab_subscriptions.plan_id                                     AS plan_id,
      COALESCE( (gitlab_subscriptions.plan_id IN {{ paid_plans }} ), False)
                                                                       AS namespace_plan_is_paid,
      COALESCE(COUNT(DISTINCT members.member_id), 0)                   AS member_count,
      COALESCE(COUNT(DISTINCT projects.project_id), 0)                 AS project_count

    FROM namespaces
      LEFT JOIN members
        ON namespaces.namespace_id = members.source_id
        AND members.member_source_type = 'Namespace'
      LEFT JOIN projects
        ON namespaces.namespace_id = projects.namespace_id
      LEFT JOIN namespace_lineage
        ON namespaces.namespace_id = namespace_lineage.namespace_id
      LEFT JOIN gitlab_subscriptions
        ON namespaces.namespace_id = gitlab_subscriptions.namespace_id
        AND gitlab_subscriptions.rank_in_namespace_id = 1
    {{ dbt_utils.group_by(n=28) }}
)

SELECT *
FROM joined
