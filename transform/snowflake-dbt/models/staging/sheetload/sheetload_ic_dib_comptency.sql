WITH source AS (

	SELECT *
	FROM {{ ref('sheetload_ic_dib_comptency_source') }}

),    

{{cleanup_certificates("'ic_dib_comptency'")}}


