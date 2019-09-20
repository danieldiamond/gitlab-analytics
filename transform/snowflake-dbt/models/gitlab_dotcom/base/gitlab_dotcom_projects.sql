{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'projects') }}

), renamed AS (

    SELECT

      id::INTEGER                                                                 AS project_id,
      description :: varchar                                                        AS project_description,
      import_source :: varchar                                                      AS project_import_source,
      issues_template :: varchar                                                    AS project_issues_template,
      build_coverage_regex                                                          AS project_build_coverage_regex,      
      name :: varchar                                                               AS project_name,
      path :: varchar                                                               AS project_path,
      import_url                                                                    AS project_import_url,
      merge_requests_template                                                       AS project_merge_requests_template,

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

      archived :: boolean                                                           AS archived,

      IFF(avatar IS NULL, FALSE, TRUE)                                              AS has_avatar,

      star_count::INTEGER                                                         AS project_star_count,
      merge_requests_rebase_enabled :: boolean                                      AS merge_requests_rebase_enabled,
      IFF(LOWER(import_type) = 'nan', NULL, import_type)                            AS import_type,
      approvals_before_merge::INTEGER                                             AS approvals_before_merge,
      reset_approvals_on_push :: boolean                                            AS reset_approvals_on_push,
      merge_requests_ff_only_enabled :: boolean                                     AS merge_requests_ff_only_enabled,
      mirror :: boolean                                                             AS mirror,
      mirror_user_id::INTEGER                                                     AS mirror_user_id,
      shared_runners_enabled :: boolean                                             AS shared_runners_enabled,
      build_allow_git_fetch :: boolean                                              AS build_allow_git_fetch,
      build_timeout::INTEGER                                                      AS build_timeout,
      mirror_trigger_builds :: boolean                                              AS mirror_trigger_builds,
      pending_delete :: boolean                                                     AS pending_delete,
      public_builds :: boolean                                                      AS public_builds,
      last_repository_check_failed :: boolean                                       AS last_repository_check_failed,
      last_repository_check_at :: timestamp                                         AS last_repository_check_at,
      container_registry_enabled :: boolean                                         AS container_registry_enabled,
      only_allow_merge_if_pipeline_succeeds :: boolean                              AS only_allow_merge_if_pipeline_succeeds,
      has_external_issue_tracker :: boolean                                         AS has_external_issue_tracker,
      repository_storage,
      repository_read_only :: boolean                                               AS repository_read_only,
      request_access_enabled :: boolean                                             AS request_access_enabled,
      has_external_wiki :: boolean                                                  AS has_external_wiki,
      ci_config_path,
      lfs_enabled :: boolean                                                        AS lfs_enabled,
      only_allow_merge_if_all_discussions_are_resolved :: boolean                   AS only_allow_merge_if_all_discussions_are_resolved,
      repository_size_limit::INTEGER                                              AS repository_size_limit,
      printing_merge_request_link_enabled :: boolean                                AS printing_merge_request_link_enabled,
      IFF(auto_cancel_pending_pipelines :: int = 1, TRUE, FALSE)                    AS has_auto_canceling_pending_pipelines,
      service_desk_enabled :: boolean                                               AS service_desk_enabled,
      IFF(LOWER(delete_error) = 'nan', NULL, delete_error)                          AS delete_error,
      last_repository_updated_at :: timestamp                                       AS last_repository_updated_at,
      storage_version::INTEGER                                                    AS storage_version,
      resolve_outdated_diff_discussions :: boolean                                  AS resolve_outdated_diff_discussions,
      disable_overriding_approvers_per_merge_request :: boolean                     AS disable_overriding_approvers_per_merge_request,
      remote_mirror_available_overridden :: boolean                                 AS remote_mirror_available_overridden,
      only_mirror_protected_branches :: boolean                                     AS only_mirror_protected_branches,
      pull_mirror_available_overridden :: boolean                                   AS pull_mirror_available_overridden,
      mirror_overwrites_diverged_branches :: boolean                                AS mirror_overwrites_diverged_branches,
      external_authorization_classification_label
    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
