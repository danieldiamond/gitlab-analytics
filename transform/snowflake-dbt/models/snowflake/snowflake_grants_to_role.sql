WITH source AS (

    SELECT *
    FROM {{ source('snowflake','grants_to_role') }}

), intermediate AS (

    SELECT
      name                                      AS role_name,
      created_on,
      privilege,
      granted_on,
      granted_to                                AS granted_to_type,
      grantee_name                              AS grantee_user_name,
      grant_option,
      granted_by,
      to_timestamp_ntz(_uploaded_at::number)    AS snapshot_date
    FROM source

), max_select AS (

    SELECT *
    FROM intermediate
    WHERE snapshot_date = (SELECT max(snapshot_date) FROM intermediate)

)

SELECT *
FROM max_select
