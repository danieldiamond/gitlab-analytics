WITH source AS (

    SELECT *
    FROM {{ var("database") }}.salesforce_stitch.campaign

), renamed AS(

    SELECT
        id as campaign_id,
        name as campaign_name,
        isactive as is_active,
        startdate as start_date,
        enddate as end_date,
        status as status,
        type as type,

        --keys
        campaignmemberrecordtypeid as campaign_member_record_type_id,
        ownerid as campaign_owner_id,
        parentid as campaign_parent_id,

        --info
        description as description,

        --projections
        budgetedcost as budgeted_cost,
        expectedresponse as expected_response,
        expectedrevenue as expected_revenue,

        --results
        actualcost as actual_cost,
        amountallopportunities as amount_all_opportunities,
        amountwonopportunities as amount_won_opportunities,
        numberofcontacts as count_contacts,
        numberofconvertedleads as count_converted_leads,
        numberofleads as count_leads,
        numberofopportunities as count_opportunities,
        numberofresponses as count_responses,
        numberofwonopportunities as count_won_opportunities,
        numbersent as count_sent,


        --metadata
        createddate as created_date,
        createdbyid as created_by_id,
        lastmodifiedbyid as last_modified_by_id,
        lastmodifieddate as last_modified_date,
        lastactivitydate as last_activity_date,
        systemmodstamp

    FROM source
    WHERE isdeleted = FALSE

)

SELECT *
FROM renamed
