WITH source AS (

    SELECT *
    FROM {{ source('rspec', 'profiling_data') }}

),

renamed AS (

    SELECT


      commit				    ,
      commit_time			    ,
      total_time			    ,
      number_of_tests		    ,
      time_per_single_test      ,
      total_queries		        ,
      total_query_time	        ,
      total_requests		    ,
      _UPDATED_AT      as updated_at
    FROM source

)

SELECT *
FROM renamed

