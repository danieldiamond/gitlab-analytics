WITH source as (

    SELECT *
    FROM {{ source('gitlab_snowplow', 'events') }}

)

SELECT *
FROM source
