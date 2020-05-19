WITH session_custom_dims AS (

	SELECT *
	FROM {{ ref('ga360_session_custom_dimension') }}

), ga_index_names AS (

	SELECT * 
	FROM  {{ ref('google_analytics_custom_dimension_indexes') }}

), named_dims AS(

	SELECT
	  --dimensions
	  session_custom_dims.*,
	    
	  --index names
	  ga_index_names.name	AS dimension_name
	    
	FROM session_custom_dims
	LEFT JOIN ga_index_names 
		ON session_custom_dims.dimension_index = ga_index_names.index

)

SELECT *
FROM named_dims
