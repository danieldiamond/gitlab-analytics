{{
    config(
        materialized='incremental',
        unique_key='snowflake_query_id'
    )
}}

with source AS(
    SELECT *
    FROM {{source('snowflake_account_usage','query_history')}}
)

SELECT 
query_id 			AS snowflake_query_id,

-- Foreign Keys
database_id			AS snowflake_database_id,
schema_id			AS snowflake_schema_id,
session_id			AS snowflake_session_id,
warehouse_id		AS snowflake_warehouse_id,


-- Logical Info
database_name		AS snowflake_database_name,
query_text			AS snowflake_query_text,
role_name			AS snowflake_role_name,
rows_produced		AS snowflake_rows_produced,
schema_name			AS snowflake_schema_name,
user_name			AS snowflake_user_name,
warehouse_name		AS snowflake_warehouse_name,


-- metadata 
end_time			AS snowflake_query_end_time,
start_time			AS snowflake_query_start_time
FROM source


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE snowflake_query_start_time > (SELECT MAX(snowflake_query_start_time) from {{ this }})

{% endif %}

limit 100 --delete me