{{ config({
    "materialized": "table"
    })
}}

WITH source AS (

  SELECT
    *,
    DENSE_RANK() OVER (ORDER BY DATEADD(S, _uploaded_at, '1970-01-01')::DATE ) AS rank_in_uploaded_date
  FROM {{ source('gitlab_dotcom', 'label_links') }}

), renamed AS (

    SELECT

      id::INTEGER                                    AS label_link_id,
      label_id::INTEGER                              AS label_id,
      target_id::INTEGER                             AS target_id,
      target_type,
      created_at::TIMESTAMP                          AS label_link_created_at,
      updated_at::TIMESTAMP                          AS label_link_updated_at

    FROM source
    WHERE rank_in_uploaded_date = 1

)

SELECT *
FROM renamed
