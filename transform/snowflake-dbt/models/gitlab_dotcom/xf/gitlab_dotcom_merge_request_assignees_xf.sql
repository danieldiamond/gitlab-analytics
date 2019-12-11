WITH users AS (

    SELECT
      user_id,
      username
    FROM {{ ref('gitlab_dotcom_users') }}

), merge_requests AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_requests_xf') }}

), notes AS (

    SELECT
      *,
      REPLACE(SPLIT_PART(note, 'unassigned ', 1), 'assigned to ', '') AS assigned,
      SPLIT_PART(note, 'unassigned ', 2)                              AS unassigned
    FROM {{ ref('gitlab_dotcom_internal_notes_xf') }}
    WHERE noteable_type = 'MergeRequest'
      AND (note LIKE 'assigned%' OR note like 'unassigned%')

), notes_cleaned AS (

  SELECT
    note_id,
    noteable_id,
    note_author_id,
    created_at,
    note,
    event,
    strtok_to_array(REGEXP_REPLACE(event_string, '(, and )|( and )|(, )', ','), ',') AS event_cleaned
  FROM note_sample
  UNPIVOT(event_string FOR event IN (assigned, unassigned))
  
), notes_flat AS (

  SELECT 
    note_id,
    noteable_id,
    note_author_id,
    created_at,
    note,
    event,
    f.index                   AS rank_in_event,
    REPLACE(f.value, '@', '') AS username
  FROM notes_cleaned,
  LATERAL FLATTEN(input => event_cleaned) f

), joined AS (

  SELECT
    merge_request_id,
    merge_request_created_at,
    merged_at,
    author_id  AS merge_request_author_id,
    note_author_id,
    created_at AS note_created_at,
    event,
    user_id    AS event_user_id,
    rank_in_event,
    namespace_id,
    ultimate_parent_id,
    is_included_in_engineering_metrics,
    is_part_of_product,
    is_community_contributor_related
  FROM notes_flat
  LEFT JOIN users
    ON notes_flat.username = users.username
  LEFT JOIN merge_requests
    ON notes_flat.noteable_id = merge_requests.merge_request_id  
  
)

SELECT *
FROM joined