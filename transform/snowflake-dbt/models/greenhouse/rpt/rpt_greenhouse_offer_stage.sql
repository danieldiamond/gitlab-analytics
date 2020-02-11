WITH gh_offers AS (
  
    SELECT *
    FROM {{ref('greenhouse_offers')}}

), gh_applications AS (
    
    SELECT *
    FROM {{ref('greenhouse_applications')}}
  
), greenhouse_offer_custom_fields as (


    SELECT 
        offer_id, 
        offer_custom_field,
        offer_custom_field_display_value
    FROM {{ref('greenhouse_offer_custom_fields')}} 
    WHERE offer_custom_field IN ('Candidate City', 'Candidate State', 'Candidate Country')

), greenhouse_location_fields AS (
    
    SELECT *
    FROM greenhouse_offer_custom_fields
    PIVOT(MAX(offer_custom_field_display_value) FOR offer_custom_field IN ('Candidate City', 'Candidate State', 'Candidate Country'))
          as p (offer_id, candidate_city, candidate_state, candidate_country)

), final AS (

    SELECT 
      gh_offers.offer_id,
      application_status, 
      stage_name, 
      offer_status,
      gh_offers.created_at,
      gh_offers.sent_at,
      gh_offers.start_date,
      candidate_state,
      candidate_country
    FROM gh_offers 
    INNER JOIN gh_applications 
      ON gh_offers.application_id = gh_applications.application_id
    LEFT JOIN greenhouse_location_fields 
      ON gh_offers.offer_id = greenhouse_location_fields.offer_id

)

SELECT * 
FROM final