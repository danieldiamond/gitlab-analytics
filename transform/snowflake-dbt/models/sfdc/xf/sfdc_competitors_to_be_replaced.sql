with base as (

    SELECT  opportunity_id, 
            competitors,
            comps.*
    FROM {{ref('sfdc_opportunity_xf')}},
    lateral flatten(input =>split(competitors, ';'), outer => true) comps

), competitors_unpacked AS (

    SELECT  opportunity_id, 
            competitors,
            value::string as competitors_to_be_replaced_unpacked
    FROM base

)

SELECT *
FROM competitors_unpacked