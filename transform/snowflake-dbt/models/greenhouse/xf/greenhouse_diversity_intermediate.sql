WITH date_details AS (
  
    SELECT 
      date_actual                                                          AS month_date,                               
      'join'                                                               AS join_field  
    FROM {{ ref ('date_details') }}
    WHERE date_actual <= {{max_date_in_bamboo_analyses()}}
      AND day_of_month = 1 
      AND date_actual >= '2018-09-01' -- 1st date we started capturing eeoc data


), greenhouse_recruiting_xf AS (

    SELECT *
    FROM  {{ ref ('greenhouse_recruiting_xf') }}

), eeoc AS (
      {{ dbt_utils.unpivot(
      relation=ref('greenhouse_eeoc_responses'),
      cast_to='varchar',
      exclude=['application_id'],
      remove=['eeoc_response_submitted_at'],
      field_name='eeoc_field_name',
      value_name='eeoc_values'
      ) }}

), eeoc_fields AS (
    SELECT DISTINCT 
      eeoc_field_name                                  AS eeoc_field_name,
      'join'                                           AS join_field
    FROM eeoc

), base AS (
    SELECT
      month_date,
      eeoc_field_name
    FROM date_details
    LEFT JOIN eeoc_fields 
      ON eeoc_fields.join_field = date_details.join_field

)

select * from base 


{# ), greenhouse_diversity_intermediate     

  
    SELECT 
      base.*,
      applications.candidate_id,
      IFF(eeoc_values in ('I don''t wish to answer','Decline To Self Identify'), 
            'Did Not Identify',
            COALESCE(eeoc_values, 'Did Not Identify'))                                  AS eeoc_values
     -- COUNT(DISTINCT(applications.application_id))                                      AS total_applications,
     -- SUM(IFF(offers.offer_status = 'accepted',1,0))                                    AS accepted_offer
    FROM greenhouse_recruiting_xf
    LEFT JOIN base
      ON date_trunc('month',applications.applied_at) = base.month_date
    LEFT JOIN eeoc            
      ON applications.application_id = eeoc.application_id
      AND eeoc.eeoc_field_name = base.eeoc_field_name

) 

SELECT *
FROM greenhouse_diversity_intermediate #} 