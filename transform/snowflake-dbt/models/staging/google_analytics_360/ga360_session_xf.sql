WITH sessions AS (

	SELECT *
	FROM {{ ref('ga360_session') }}

), session_custom_dims AS (

	SELECT *
	FROM {{ ref('ga360_session_custom_dimension') }}

), sessions_xf AS(

	SELECT
	    
	  --sessions
	  sessions.*,
	    
	  --client_id dimension
	  clientid_dim.dimension_value  AS client_id
		
	FROM sessions
	LEFT JOIN session_custom_dims AS clientid_dim
		ON sessions.visit_id = clientid_dim.visit_id
		    and clientid_dim.dimension_index = 6 --clientID
		    
)

SELECT *
FROM sessions_xf