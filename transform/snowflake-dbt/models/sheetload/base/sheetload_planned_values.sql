{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'planned_values') }}

), renamed AS (


	SELECT
       unique_key::INT as primary_key,
       plan_month::DATE as plan_month,
	     planned_new_pipe::INT as planned_new_pipe,
	     planned_total_iacv::INT as planned_total_iacv,
	     planned_tcv_minus_gross_opex::INT as planned_tcv_minus_gross_opex

	FROM source

)

SELECT * FROM renamed

