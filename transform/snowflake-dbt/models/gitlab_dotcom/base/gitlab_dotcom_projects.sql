{{ config(schema='analytics') }}

{% set sensitive_fields = ['description', 'import_source','issues_template','build_coverage_regex'] %}

{% set fields_to_mask = ['name', 'path','import_url','merge_requests_template'] %}

WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.projects

), renamed AS (

    SELECT

      id :: integer                                                                 as project_id,

      {% for field in fields_to_mask %}
      IFF(visibility_level != '20',
          'Project is private/internal', {{field}})                                 as project_{{field}},
      {% endfor %}

      {% for field in sensitive_fields %}

      CASE
        WHEN visibility_level != '20'
          THEN 'project is private/internal'
        WHEN visibility_level = '20' AND {{field}} = 'nan'
          THEN NULL
        WHEN visibility_level = '20' AND {{field}} != 'nan'
          THEN {{field}}
      END
                                                                                    as project_{{field}},
      {% endfor %}

      created_at :: timestamp                                                       as project_created_at,
      updated_at :: timestamp                                                       as project_updated_at,

      creator_id :: number                                                          as creator_id,
      namespace_id :: number                                                        as namespace_id,

      last_activity_at :: timestamp                                                 as last_activity_at,

      CASE
        WHEN visibility_level = '20' THEN 'public'
        WHEN visibility_level = '10' THEN 'internal'
        ELSE 'private'
      END                                                                           as visibility_level,

      TRY_CAST(archived as boolean)                                                 as archived,

      IFF(avatar IS NULL, FALSE, TRUE)                                              as has_avatar,

      import_status,
      star_count :: integer                                                         as project_star_count,
      TRY_CAST(merge_requests_rebase_enabled as boolean)                            as merge_requests_rebase_enabled,
      IFF(lower(import_type) = 'nan', NULL, import_type)                            as import_type,
      approvals_before_merge :: integer                                             as approvals_before_merge,
      TRY_CAST(reset_approvals_on_push as boolean)                                  as reset_approvals_on_push,
      TRY_CAST(merge_requests_ff_only_enabled as boolean)                           as merge_requests_ff_only_enabled,
      TRY_CAST(mirror as boolean)                                                   as mirror,
      TRY_CAST(mirror_user_id as integer)                                           as mirror_user_id,
      TRY_CAST(ci_id as integer)                                                    as ci_id,
      TRY_CAST(shared_runners_enabled as boolean)                                   as shared_runners_enabled,
      TRY_CAST(build_allow_git_fetch as boolean)                                    as build_allow_git_fetch,
      TRY_CAST(build_timeout as integer)                                            as build_timeout,
      TRY_CAST(mirror_trigger_builds as boolean)                                    as mirror_trigger_builds,
      TRY_CAST(pending_delete as boolean)                                           as pending_delete,
      TRY_CAST(public_builds as boolean)                                            as public_builds,
      TRY_CAST(last_repository_check_failed as boolean)                             as last_repository_check_failed,
      TRY_CAST(last_repository_check_at as timestamp)                               as last_repository_check_at,
      TRY_CAST(container_registry_enabled as boolean)                               as container_registry_enabled,
      TRY_CAST(only_allow_merge_if_pipeline_succeeds as boolean)                    as only_allow_merge_if_pipeline_succeeds,
      TRY_CAST(has_external_issue_tracker as boolean)                               as has_external_issue_tracker,
      repository_storage,
      TRY_CAST(repository_read_only as boolean)                                     as repository_read_only,
      TRY_CAST(request_access_enabled as boolean)                                   as request_access_enabled,
      TRY_CAST(has_external_wiki as boolean)                                        as has_external_wiki,
      ci_config_path,
      TRY_CAST(lfs_enabled as boolean)                                              as lfs_enabled,
      TRY_CAST(only_allow_merge_if_all_discussions_are_resolved as boolean)         as only_allow_merge_if_all_discussions_are_resolved,
      TRY_CAST(repository_size_limit as int)                                        as repository_size_limit,
      TRY_CAST(printing_merge_request_link_enabled as boolean)                      as printing_merge_request_link_enabled,
      IFF(auto_cancel_pending_pipelines :: int = 1, TRUE, FALSE)                    as has_auto_canceling_pending_pipelines,
      TRY_CAST(service_desk_enabled as boolean)                                     as service_desk_enabled,
      IFF(lower(delete_error) = 'nan', NULL, delete_error)                          as delete_error,
      TRY_CAST(last_repository_updated_at as timestamp)                             as last_repository_updated_at,
      TRY_CAST(storage_version as integer)                                          as storage_version,
      TRY_CAST(resolve_outdated_diff_discussions as boolean)                        as resolve_outdated_diff_discussions,
      TRY_CAST(disable_overriding_approvers_per_merge_request as boolean)           as disable_overriding_approvers_per_merge_request,
      TRY_CAST(remote_mirror_available_overridden as boolean)                       as remote_mirror_available_overridden,
      TRY_CAST(only_mirror_protected_branches as boolean)                           as only_mirror_protected_branches,
      TRY_CAST(pull_mirror_available_overridden as boolean)                         as pull_mirror_available_overridden,
      TRY_CAST(mirror_overwrites_diverged_branches as boolean)                      as mirror_overwrites_diverged_branches,
      TRY_CAST(external_authorization_classification_label as boolean)              as external_authorization_classification_label,
      TO_TIMESTAMP(cast(_updated_at as int))                                        as projects_last_updated_at
    FROM source


)

SELECT *
FROM renamed