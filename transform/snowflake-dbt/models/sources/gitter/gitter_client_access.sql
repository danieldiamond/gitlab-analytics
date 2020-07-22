{{ config({
    "schema": "staging"
    })
}}

with source as (

  select *
  from {{ var("database") }}.GITTER.GITTER_CLIENT_ACCESS_EVENTS

), renamed as (

  select
  -- id
  _id as id,

  -- data
  d__env as env,
  to_timestamp_tz(t) as event_at,

  d__user_id as user_id,
  d__client_id as client_id,
  d__client_name as client_name,
  d__client_key as client_key,
  d__tag as client_tag

  --agent_type,
  --agent_family,
  --agent_device_family
  --agent_os_family

  from source

)

select *
from renamed
