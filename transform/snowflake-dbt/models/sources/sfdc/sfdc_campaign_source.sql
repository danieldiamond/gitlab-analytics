WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'campaign') }}



), renamed AS(

    SELECT
        id                                                                 AS campaign_id,
        name                                                                AS campaign_name,
        isactive                                                            AS is_active,
        startdate                                                           AS start_date,
        enddate                                                             AS end_date,
        status                                                              AS status,
        type                                                                AS type,

        --keys
        campaignmemberrecordtypeid                                          AS campaign_member_record_type_id,
        ownerid                                                             AS campaign_owner_id,
        parentid                                                            AS campaign_parent_id,

        --info
        description                                                         AS description,
        region__c                                                           AS region,
        sub_region__c                                                       AS sub_region,

        --projections
        budgetedcost                                                        AS budgeted_cost,
        expectedresponse                                                    AS expected_response,
        expectedrevenue                                                     AS expected_revenue,

        --results
        actualcost                                                          AS actual_cost,
        amountallopportunities                                              AS amount_all_opportunities,
        amountwonopportunities                                              AS amount_won_opportunities,
        numberofcontacts                                                    AS count_contacts,
        numberofconvertedleads                                              AS count_converted_leads,
        numberofleads                                                       AS count_leads,
        numberofopportunities                                               AS count_opportunities,
        numberofresponses                                                   AS count_responses,
        numberofwonopportunities                                            AS count_won_opportunities,
        numbersent                                                          AS count_sent,


        --metadata
        createddate                                                         AS created_date,
        createdbyid                                                         AS created_by_id,
        lastmodifiedbyid                                                    AS last_modified_by_id,
        lastmodifieddate                                                    AS last_modified_date,
        lastactivitydate                                                    AS last_activity_date,
        systemmodstamp,

        isdeleted                                                           AS is_deleted

    FROM source
)

SELECT *
FROM renamed
