{{ config({
    "schema": "sensitive",
    "materialized": "table"
    })
}}

{% set repeated_column_names = 
    "greenhouse_recruiting_xf.application_id,
      department_name::VARCHAR(100)                                                 AS department_name,
      division::VARCHAR(100)                                                        AS division,
      division_modified::VARCHAR(100)                                               AS division_modified,
      source_type::VARCHAR(100)                                                     AS source_type,
      CASE WHEN eeoc_values in ('I don''t wish to answer','Decline To Self Identify') 
            THEN 'did not identify'
           WHEN eeoc_values = 'No, I don''t have a disability' 
            THEN 'No' 
            ELSE COALESCE(lower(eeoc_values), 'did not identify') end                AS eeoc_values
" %}

WITH date_details AS (
  
    SELECT 
      date_actual                                                          AS month_date,                               
      'join'                                                               AS join_field  
    FROM {{ ref ('date_details') }}
    WHERE date_actual <= {{max_date_in_bamboo_analyses()}}
      AND day_of_month = 1 
      AND date_actual >= '2018-08-12' -- 1st date we started capturing eeoc data

), greenhouse_recruiting_xf AS (

    SELECT *
    FROM  {{ ref ('greenhouse_recruiting_xf') }}

), eeoc_base as (
  
  SELECT application_id, candidate_status, candidate_race, candidate_gender, candidate_disability_status, candidate_veteran_status 
  FROM "ANALYTICS"."ANALYTICS_SENSITIVE"."GREENHOUSE_EEOC_RESPONSES"

), eeoc as (

    select * from eeoc_base
    unpivot(eeoc_values for eeoc_field_name in (candidate_status, candidate_race, candidate_gender, candidate_disability_status, candidate_veteran_status ))


), eeoc_fields AS (

    SELECT DISTINCT 
      lower(eeoc_field_name)::VARCHAR(50)             AS eeoc_field_name,
      'join'                                           AS join_field
    FROM eeoc

), base AS (

    SELECT
      month_date,
      eeoc_field_name
    FROM date_details
    LEFT JOIN eeoc_fields 
      ON eeoc_fields.join_field = date_details.join_field  
  
    UNION ALL

    SELECT
      month_date,
      'no_eeoc' AS eeoc_field_name
    FROM date_details
    
 ), applications AS (

    SELECT 
      base.*,
      'application_month'                                                               AS capture_month,
      {{repeated_column_names}},
      IFF(offer_status = 'accepted',1,0)                                                AS accepted_offer,
      null                                                                              AS time_to_offer,
      IFF(sourced_candidate = TRUE, 1,0)                                                AS sourced_candidate,
      IFF(sourced_candidate = TRUE AND offer_status = 'accepted', 1,0)                  AS hired_sourced_candidate
    FROM base
    LEFT JOIN greenhouse_recruiting_xf
      ON DATE_TRUNC('month',greenhouse_recruiting_xf.application_date) = base.month_date
    LEFT JOIN eeoc            
      ON greenhouse_recruiting_xf.application_id = eeoc.application_id
      AND LOWER(eeoc.eeoc_field_name) = base.eeoc_field_name  
 )
{# 
), offers AS (

    SELECT 
      base.*,
      'offer_sent_month'                                                               AS capture_month,
      {{repeated_column_names}},
      IFF(offer_status = 'accepted',1,0)                                                AS accepted_offer,
      null                                                                              AS time_to_offer,
      IFF(sourced_candidate = TRUE, 1,0)                                                AS sourced_candidate,
      IFF(sourced_candidate = TRUE AND offer_status = 'accepted', 1,0)                  AS hired_sourced_candidate
    FROM base
    LEFT JOIN greenhouse_recruiting_xf
      ON DATE_TRUNC('month',greenhouse_recruiting_xf.offer_sent_date) = base.month_date
    LEFT JOIN eeoc            
      ON greenhouse_recruiting_xf.application_id = eeoc.application_id
      AND LOWER(eeoc.eeoc_field_name) = base.eeoc_field_name 
    WHERE offer_status IS NOT NULL


) select * from offers    #}


    SELECT 
      base.*,
      'offer_sent_month'                                                               AS capture_month,
      {{repeated_column_names}}
    FROM base
    LEFT JOIN greenhouse_recruiting_xf
      ON DATE_TRUNC('month', greenhouse_recruiting_xf.offer_sent_date) = base.month_date
    LEFT JOIN eeoc            
      ON greenhouse_recruiting_xf.application_id = eeoc.application_id
      AND LOWER(eeoc.eeoc_field_name) = base.eeoc_field_name 
    WHERE offer_status IS NOT NULL
