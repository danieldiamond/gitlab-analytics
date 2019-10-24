WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'planned_values') }}

), renamed AS (

	SELECT
		unique_key::INT                    	AS primary_key,
		plan_month::DATE                    AS plan_month,
		planned_new_pipe::INT               AS planned_new_pipe,
		planned_total_iacv::INT             AS planned_total_iacv,
		planned_tcv_minus_gross_opex::INT   AS planned_tcv_minus_gross_opex,
		planned_total_arr::INT              AS planned_total_arr,
		sales_efficiency_plan::FLOAT        AS sales_efficiency_plan,
		magic_number_plan::FLOAT            AS magic_number_plan,
		tcv_plan::INT                       AS tcv_plan
	FROM source

)

SELECT *
FROM renamed
