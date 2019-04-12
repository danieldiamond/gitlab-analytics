{{ config(schema='analytics') }}

{% set sensitive_fields = ['description', 'import_source','issues_template','build_coverage_regex','name', 'path','import_url','merge_requests_template'] %}
{% set gitlab_namespaces = (6543,9970,4347861) %}

WITH source as (

  SELECT *
  FROM {{ var("database") }}.gitlab_dotcom.projects

), renamed as (

    SELECT

      id :: integer                                                                 AS project_id,

      {% for field in sensitive_fields %}
      CASE
        WHEN visibility_level != '20' AND namespace_id::int NOT IN {{gitlab_namespaces}}
          THEN 'project is private/internal'
        ELSE {{field}}
      END                                                                           AS project_{{field}},
      {% endfor %}

      created_at :: timestamp                                                       AS project_created_at,
      updated_at :: timestamp                                                       AS project_updated_at,

      creator_id :: number                                                          AS creator_id,
      namespace_id :: number                                                        AS namespace_id,

      last_activity_at :: timestamp                                                 AS last_activity_at,

      CASE
        WHEN visibility_level = '20' THEN 'public'
        WHEN visibility_level = '10' THEN 'internal'
        ELSE 'private'
      END                                                                           AS visibility_level,

      TRY_CAST(archived AS boolean)                                                 AS archived,

      IFF(avatar IS NULL, FALSE, TRUE)                                              AS has_avatar,

      import_status,
      star_count :: integer                                                         AS project_star_count,
      TRY_CAST(merge_requests_rebase_enabled AS boolean)                            AS merge_requests_rebase_enabled,
      IFF(LOWER(import_type) = 'nan', NULL, import_type)                            AS import_type,
      approvals_before_merge :: integer                                             AS approvals_before_merge,
      TRY_CAST(reset_approvals_on_push AS boolean)                                  AS reset_approvals_on_push,
      TRY_CAST(merge_requests_ff_only_enabled AS boolean)                           AS merge_requests_ff_only_enabled,
      TRY_CAST(mirror AS boolean)                                                   AS mirror,
      TRY_CAST(mirror_user_id AS integer)                                           AS mirror_user_id,
      TRY_CAST(ci_id AS integer)                                                    AS ci_id,
      TRY_CAST(shared_runners_enabled AS boolean)                                   AS shared_runners_enabled,
      TRY_CAST(build_allow_git_fetch AS boolean)                                    AS build_allow_git_fetch,
      TRY_CAST(build_timeout AS integer)                                            AS build_timeout,
      TRY_CAST(mirror_trigger_builds AS boolean)                                    AS mirror_trigger_builds,
      TRY_CAST(pending_delete AS boolean)                                           AS pending_delete,
      TRY_CAST(public_builds AS boolean)                                            AS public_builds,
      TRY_CAST(last_repository_check_failed AS boolean)                             AS last_repository_check_failed,
      TRY_CAST(last_repository_check_at AS timestamp)                               AS last_repository_check_at,
      TRY_CAST(container_registry_enabled AS boolean)                               AS container_registry_enabled,
      TRY_CAST(only_allow_merge_if_pipeline_succeeds AS boolean)                    AS only_allow_merge_if_pipeline_succeeds,
      TRY_CAST(has_external_issue_tracker AS boolean)                               AS has_external_issue_tracker,
      repository_storage,
      TRY_CAST(repository_read_only AS boolean)                                     AS repository_read_only,
      TRY_CAST(request_access_enabled AS boolean)                                   AS request_access_enabled,
      TRY_CAST(has_external_wiki as boolean)                                        AS has_external_wiki,
      ci_config_path,
      TRY_CAST(lfs_enabled AS boolean)                                              AS lfs_enabled,
      TRY_CAST(only_allow_merge_if_all_discussions_are_resolved AS boolean)         AS only_allow_merge_if_all_discussions_are_resolved,
      TRY_CAST(repository_size_limit AS int)                                        AS repository_size_limit,
      TRY_CAST(printing_merge_request_link_enabled AS boolean)                      AS printing_merge_request_link_enabled,
      IFF(auto_cancel_pending_pipelines :: int = 1, TRUE, FALSE)                    AS has_auto_canceling_pending_pipelines,
      TRY_CAST(service_desk_enabled AS boolean)                                     AS service_desk_enabled,
      IFF(LOWER(delete_error) = 'nan', NULL, delete_error)                          AS delete_error,
      TRY_CAST(last_repository_updated_at AS timestamp)                             AS last_repository_updated_at,
      TRY_CAST(storage_version AS integer)                                          AS storage_version,
      TRY_CAST(resolve_outdated_diff_discussions AS boolean)                        AS resolve_outdated_diff_discussions,
      TRY_CAST(disable_overriding_approvers_per_merge_request AS boolean)           AS disable_overriding_approvers_per_merge_request,
      TRY_CAST(remote_mirror_available_overridden AS boolean)                       AS remote_mirror_available_overridden,
      TRY_CAST(only_mirror_protected_branches AS boolean)                           AS only_mirror_protected_branches,
      TRY_CAST(pull_mirror_available_overridden AS boolean)                         AS pull_mirror_available_overridden,
      TRY_CAST(mirror_overwrites_diverged_branches AS boolean)                      AS mirror_overwrites_diverged_branches,
      TRY_CAST(external_authorization_classification_label AS boolean)              AS external_authorization_classification_label,
      TO_TIMESTAMP(_updated_at::int)                                                AS projects_last_updated_at
    FROM source


)

SELECT *
FROM renamed