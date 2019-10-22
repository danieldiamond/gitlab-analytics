WITH resource_label_events AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_resource_label_events')}}

),

aggregated AS (

  SELECT
    label_id,

    epic_id,
    issue_id
    merge_request_id,

    MAX(CASE WHEN action_type='added'   THEN created_at END) AS max_added_at,
    MAX(CASE WHEN action_type='removed' THEN created_at END) AS max_removed_at

  FROM resource_label_events
  {{ dbt_utils.group_by(n=4) }}

),


final AS ( -- Leave removed_at NULL if less than added_at.

    SELECT
      label_id,
      epic_id,
      issue_id
      merge_request_id,
      max_added_at                                             AS added_at,
      IFF(max_removed_at > max_added_at, max_removed_at, NULL) AS removed_at,
      IFF(removed_at = NULL, 'added', 'removed')               AS latest_state
    FROM data
)

SELECT *
FROM final
