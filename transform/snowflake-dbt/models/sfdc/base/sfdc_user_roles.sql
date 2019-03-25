with base as (

    SELECT * 
    FROM {{ var("database") }}.salesforce_stitch.userrole

)

SELECT *
FROM base