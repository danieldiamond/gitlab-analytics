WITH source AS (

    SELECT *
    FROM {{ source('snowflake','roles') }}

), intermediate AS (

    SELECT
      name                                      AS role_name,      
      created_on,
      is_default,
      is_current,
      is_inherited,
      assigned_to_users,
      granted_to_roles,
      granted_roles,
      owner                                     AS owner_role,
      comment,
      to_timestamp_ntz(_uploaded_at::number)    AS snapshot_date
    FROM source

), max_select AS (

    SELECT *
    FROM intermediate
    WHERE snapshot_date = (SELECT max(snapshot_date) FROM intermediate)

)

SELECT *
FROM max_select
