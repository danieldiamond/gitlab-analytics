WITH merge_requests AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_requests_xf') }}

), notes AS (

    SELECT
      noteable_id,
      note_author_id,
      note 
    FROM {{ ref('gitlab_dotcom_notes_xf')}}

), users AS (

    SELECT 
      user_id,
      user_name
    FROM {{ ref('gitlab_dotcom_users_xf')}}

), joined_to_mr AS (

    SELECT 
      merge_requests.project_id,
      merge_requests.namespace_id,
      merge_requests.merge_request_iid,
      merge_requests.merge_request_title,
      merge_requests.merge_request_id,
      notes.note_author_id,
      users.user_name
    FROM merge_requests
    INNER JOIN notes
      ON merge_requests.merge_request_id = notes.noteable_id
    INNER JOIN users
      ON notes.note_author_id = users.user_id
    WHERE notes.note = 'merged'

)

SELECT *
FROM joined_to_mr
