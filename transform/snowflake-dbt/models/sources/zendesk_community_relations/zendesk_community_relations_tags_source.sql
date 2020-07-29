WITH source AS (

    SELECT *
    FROM {{ source('zendesk_community_relations', 'tags') }}

),

renamed AS (

    SELECT

      count AS tag_count,
      name  AS tag_name

    FROM source

)

SELECT *
FROM renamed
