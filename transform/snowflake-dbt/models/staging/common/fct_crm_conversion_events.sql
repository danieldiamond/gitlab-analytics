WITH sfdc_lead_source AS(

  SELECT *
  FROM {{ ref('sfdc_lead_source') }}

), event_lead_conversion AS (

  SELECT
    
    {{ dbt_utils.surrogate_key(['lead_history_id','field_modified_at']) }} AS event_id,
    lead_history.field_modified_at                                         AS event_timestamp,
    lead_history.lead_id                                                   AS lead_id,
    lead_history.created_by_id                                             AS crm_user_id,
    sfdc_lead_source.converted_contact_id                                  AS contact_id,
    sfdc_lead_source.converted_account_id                                  AS account_id,
    sfdc_lead_source.converted_opportunity_id                              AS opportunity_id,
    'lead conversion'                                                      AS event_name

  FROM {{ ref('sfdc_lead_history_source') }} lead_history
  INNER JOIN sfdc_lead_source
    ON sfdc_lead_source.lead_id = lead_history.lead_id

), event_marketing_qualification AS (

  SELECT
    {{ dbt_utils.surrogate_key(['lead_id','marketo_qualified_lead_datetime']) }} AS event_id,
    marketo_qualified_lead_datetime                                              AS event_timestamp,
    lead_id,
    NULL                                                                         AS crm_user_id, -- if we move this to lead history then we can get this
    converted_contact_id                                                         AS contact_id,
    converted_opportunity_id                                                     AS opportunity_id,
    converted_account_id                                                         AS account_id,
    'marketing qualification'                                                    AS event_name
  FROM sfdc_lead_source
  WHERE marketo_qualified_lead_datetime IS NOT NULL

)

SELECT *
FROM event_lead_conversion

UNION

SELECT *
FROM event_marketing_qualification