{{config({
    "schema": "staging"
  })
}}

with source as (

    SELECT *
    FROM {{ source('zendesk', 'sla_policies') }}
),

renamed as (

  SELECT id,
      created_at,
      description,
      filter['all'],
      filter['any'],
      position, title, updated_at,
      policy_metrics
  FROM RAW.ZENDESK_STITCH.SLA_POLICIES


)

SELECT *
FROM renamed
