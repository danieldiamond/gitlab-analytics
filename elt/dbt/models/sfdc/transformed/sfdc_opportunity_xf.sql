WITH source AS ( 

	SELECT * FROM {{ref('sfdc_opportunity')}}

), stages AS (

   SELECT * FROM {{ref('sfdc_opportunitystage')}}

), layered AS (

    SELECT
        source.*,
        stages.is_won AS is_won,
        stages.stage_id as opportunity_stage_id
    FROM source
    INNER JOIN stages on source.stage_name = stages.primary_label

)

SELECT *
FROM layered
