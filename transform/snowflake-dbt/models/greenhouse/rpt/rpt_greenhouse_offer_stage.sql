WITH greenhouse_offers AS (
  
    SELECT *
    FROM {{ref('greenhouse_offers')}}

), greenhouse_applications AS (
    
    SELECT *
    FROM {{ref('greenhouse_applications')}}
  
), greenhouse_offer_custom_fields as (

    SELECT 
        offer_id, 
        offer_custom_field,
        offer_custom_field_display_value                                                    AS candidate_country
    FROM {{ref('greenhouse_offer_custom_fields')}} 
    WHERE offer_custom_field = 'Candidate Country'

), zuora_regions AS (

    SELECT *
    FROM {{ref('zuora_country_geographic_region')}}

), location_cleaned AS (

    SELECT
      offer_id,
      candidate_country,
      IFF(LOWER(LEFT(candidate_country,12))= 'united state', 
          'North America',
          COALESCE(z1.geographic_region,z2.geographic_region, z3.geographic_region))       AS geographic_region      
    FROM greenhouse_offer_custom_fields
    LEFT JOIN zuora_regions z1 
      ON LOWER(z1.country_name_in_zuora) = LOWER(greenhouse_offer_custom_fields.candidate_country)
    LEFT JOIN zuora_regions z2 
      ON LOWER(z2.iso_alpha_2_code) = LOWER(greenhouse_offer_custom_fields.candidate_country)
    LEFT JOIN zuora_regions z3 
      ON LOWER(z3.iso_alpha_3_code) = LOWER(greenhouse_offer_custom_fields.candidate_country) 

), data_set AS (

    SELECT 
      gh_offers.offer_id,
      application_status, 
      stage_name, 
      offer_status,
      gh_offers.created_at,
      gh_offers.sent_at,
      gh_offers.start_date,
      geographic_region
    FROM gh_offers 
    INNER JOIN gh_applications 
      ON gh_offers.application_id = gh_applications.application_id
    INNER JOIN location_cleaned
      ON location_cleaned.offer_id = gh_offers.offer_id

), final AS (

    SELECT 
      DATE_TRUNC(WEEK,start_date)                               AS start_week,
      geographic_region,
      COUNT(offer_id)                                           AS candidates_estimated_to_start,
      SUM(IFF(offer_status = 'accepted',1,0))                   AS accepted_offers_to_start
    FROM data_set
    WHERE geographic_region = 'North America'
    GROUP BY 1,2
    ORDER BY 1 DESC

)

SELECT * 
FROM final