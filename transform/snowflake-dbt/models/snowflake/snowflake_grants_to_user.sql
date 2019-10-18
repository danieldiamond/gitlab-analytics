WITH source AS (

    SELECT *
    FROM {{ source('snowflake','grants_to_user') }}

), intermediate AS (

    SELECT
        role                                    AS role_name,
        granted_to                              AS granted_to_type,
        grantee_name,
        to_timestamp_ntz(_uploaded_at::number)  AS snapshot_date,
        created_on
    FROM source

), max_select AS (

    SELECT *
    FROM intermediate
    WHERE snapshot_date = (SELECT max(snapshot_date) FROM intermediate)

)

SELECT *
FROM max_select
