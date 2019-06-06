WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'labels') }}

),
renamed AS (

    SELECT

      id :: integer                                as label_id,
      title                                        as label_title,
      color,
      source.project_id :: integer                 as project_id,
      group_id :: integer                          as group_id,
      template,
      type                                         as label_type,
      created_at :: timestamp                      as label_created_at,
      updated_at :: timestamp                      as label_updated_at

    FROM source
    WHERE rank_in_key = 1
)

SELECT *
FROM renamed