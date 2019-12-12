WITH users AS (

    SELECT
      user_id,
      user_name
    FROM {{ ref('gitlab_dotcom_users') }}

), notes AS (

    SELECT
      *,
      CASE
        WHEN note NOT LIKE 'Reassigned%' 
          THEN REPLACE(SPLIT_PART(note, 'unassigned ', 1), 'assigned to ', '')
        ELSE NULL
      END                              AS assigned,
      CASE
        WHEN note LIKE 'Reassigned%'
          THEN REPLACE(note, 'Reassigned to ', '') 
        ELSE NULL
      END                              AS reassigned,
    SPLIT_PART(note, 'unassigned ', 2) AS unassigned
    FROM {{ ref('gitlab_dotcom_internal_notes_xf') }}
    WHERE noteable_type = 'MergeRequest'
      AND (note LIKE 'assigned to%' OR note LIKE 'unassigned%' OR note LIKE 'Reassigned%')

), notes_cleaned AS (

    SELECT
      note_id,
      noteable_id,
      note_author_id,
      created_at,
      note,
      event,
      strtok_to_array(REGEXP_REPLACE(event_string, '(, and )|( and )|(, )', ','), ',') AS event_cleaned
    FROM notes
    UNPIVOT(event_string FOR event IN (assigned, unassigned, reassigned))
  
), notes_flat AS (

    SELECT 
      note_id,
      noteable_id,
      note_author_id,
      created_at,
      note,
      LOWER(event)              AS event,
      f.index                   AS rank_in_event,
      REPLACE(f.value, '@', '') AS user_name
    FROM notes_cleaned,
    LATERAL FLATTEN(input => event_cleaned) f

), joined AS (

    SELECT
      noteable_id AS merge_request_id,
      note_id,
      note_author_id,
      created_at  AS note_created_at,
      event,
      user_id     AS event_user_id,
      rank_in_event
    FROM notes_flat 
    LEFT JOIN users
      ON notes_flat.user_name = users.user_name 
  
)

SELECT *
FROM joined
ORDER BY 1,2,7
