with base as (

    SELECT *,
     convert_timezone('America/Los_Angeles',convert_timezone('UTC',current_timestamp())) AS _last_dbt_run
    FROM {{ source('snapshots', 'sfdc_user_snapshots') }}

)

SELECT *
FROM base