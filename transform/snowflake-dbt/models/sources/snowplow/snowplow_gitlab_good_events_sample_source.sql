WITH source as (

    SELECT *
    FROM {{ source('gitlab_snowplow', 'events_sample') }}

)

SELECT *
FROM source
