-- depends on: {{ ref('snowplow_sessions') }}

{{ schema_union_limit('snowplow_', 'snowplow_unnested_events', 'derived_tstamp', 30) }}
