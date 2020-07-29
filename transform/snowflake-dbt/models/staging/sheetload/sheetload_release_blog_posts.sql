WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_release_blog_posts_source') }}

)

SELECT *
FROM source