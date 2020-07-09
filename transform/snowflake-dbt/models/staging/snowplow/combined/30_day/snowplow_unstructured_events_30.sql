-- depends on: {{ ref('snowplow_sessions') }}

{{ schema_union_limit('snowplow_', 'snowplow_unstructured_events', 'derived_tstamp', 30) }}
