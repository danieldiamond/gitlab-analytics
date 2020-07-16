{{ config({
    "schema": "sensitive"
    })
}}

WITH zendesk_tickets AS (

    SELECT *
    FROM {{ref('zendesk_tickets_source')}}

), custom_fields AS (

    SELECT 
      d.value['id']     AS ticket_custom_field_id,
      d.value['value']  AS ticket_custom_field_value,
      ticket_id         AS ticket_id
    FROM zendesk_tickets,
    LATERAL FLATTEN(INPUT => ticket_custom_field_values, outer => true) d
    
)

SELECT *
FROM custom_fields
