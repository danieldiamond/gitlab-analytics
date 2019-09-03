with base as (

        SELECT  opportunity_id, 
                solutions_to_be_replaced,
                solutions.*
        FROM {{ref('sfdc_opportunity_xf')}},
        lateral flatten(input =>split(solutions_to_be_replaced, ';'), outer => true) solutions

), solutions_unpacked AS (

        SELECT  opportunity_id, 
                solutions_to_be_replaced,
                value::string as solutions_to_be_replaced_unpacked
        FROM base

)

SELECT * 
FROM solutions_unpacked