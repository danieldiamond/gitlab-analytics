{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'projects') }}

), renamed AS (

    SELECT

      id::INTEGER                                                                 AS project_id,
      description::VARCHAR                                                        AS project_description,
      import_source::VARCHAR                                                      AS project_import_source,
      issues_template::VARCHAR                                                    AS project_issues_template,
      build_coverage_regex                                                        AS project_build_coverage_regex,      
      name::VARCHAR                                                               AS project_name,
      path::VARCHAR                                                               AS project_path,
      import_url                                                                  AS project_import_url,
      merge_requests_template                                                     AS project_merge_requests_template,

      created_at::TIMESTAMP                                                       AS project_created_at,
      updated_at::TIMESTAMP                                                       AS project_updated_at,

      creator_id :: number                                                        AS creator_id,
      namespace_id :: number                                                      AS namespace_id,

      last_activity_at::TIMESTAMP                                                 AS last_activity_at,

      CASE
        WHEN visibility_level = '20' THEN 'public'
        WHEN visibility_level = '10' THEN 'internal'
        ELSE 'private'
      END                                                                         AS visibility_level,

      archived::BOOLEAN                                                           AS archived,

      IFF(avatar IS NULL, FALSE, TRUE)                                            AS has_avatar,

      star_count::INTEGER                                                         AS project_star_count,
      merge_requests_rebase_enabled::BOOLEAN                                      AS merge_requests_rebase_enabled,
      IFF(LOWER(import_type) = 'nan', NULL, import_type)                          AS import_type,
      approvals_before_merge::INTEGER                                             AS approvals_before_merge,
      reset_approvals_on_push::BOOLEAN                                            AS reset_approvals_on_push,
      merge_requests_ff_only_enabled::BOOLEAN                                     AS merge_requests_ff_only_enabled,
      mirror::BOOLEAN                                                             AS mirror,
      mirror_user_id::INTEGER                                                     AS mirror_user_id,
      shared_runners_enabled::BOOLEAN                                             AS shared_runners_enabled,
      build_allow_git_fetch::BOOLEAN                                              AS build_allow_git_fetch,
      build_timeout::INTEGER                                                      AS build_timeout,
      mirror_trigger_builds::BOOLEAN                                              AS mirror_trigger_builds,
      pending_delete::BOOLEAN                                                     AS pending_delete,
      public_builds::BOOLEAN                                                      AS public_builds,
      last_repository_check_failed::BOOLEAN                                       AS last_repository_check_failed,
      last_repository_check_at::TIMESTAMP                                         AS last_repository_check_at,
      container_registry_enabled::BOOLEAN                                         AS container_registry_enabled,
      only_allow_merge_if_pipeline_succeeds::BOOLEAN                              AS only_allow_merge_if_pipeline_succeeds,
      has_external_issue_tracker::BOOLEAN                                         AS has_external_issue_tracker,
      repository_storage,
      repository_read_only::BOOLEAN                                               AS repository_read_only,
      request_access_enabled::BOOLEAN                                             AS request_access_enabled,
      has_external_wiki::BOOLEAN                                                  AS has_external_wiki,
      ci_config_path,
      lfs_enabled::BOOLEAN                                                        AS lfs_enabled,
      only_allow_merge_if_all_discussions_are_resolved::BOOLEAN                   AS only_allow_merge_if_all_discussions_are_resolved,
      repository_size_limit::INTEGER                                              AS repository_size_limit,
      printing_merge_request_link_enabled::BOOLEAN                                AS printing_merge_request_link_enabled,
      IFF(auto_cancel_pending_pipelines :: int = 1, TRUE, FALSE)                  AS has_auto_canceling_pending_pipelines,
      service_desk_enabled::BOOLEAN                                               AS service_desk_enabled,
      IFF(LOWER(delete_error) = 'nan', NULL, delete_error)                        AS delete_error,
      last_repository_updated_at::TIMESTAMP                                       AS last_repository_updated_at,
      storage_version::INTEGER                                                    AS storage_version,
      resolve_outdated_diff_discussions::BOOLEAN                                  AS resolve_outdated_diff_discussions,
      disable_overriding_approvers_per_merge_request::BOOLEAN                     AS disable_overriding_approvers_per_merge_request,
      remote_mirror_available_overridden::BOOLEAN                                 AS remote_mirror_available_overridden,
      only_mirror_protected_branches::BOOLEAN                                     AS only_mirror_protected_branches,
      pull_mirror_available_overridden::BOOLEAN                                   AS pull_mirror_available_overridden,
      mirror_overwrites_diverged_branches::BOOLEAN                                AS mirror_overwrites_diverged_branches,
      external_authorization_classification_label
    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
