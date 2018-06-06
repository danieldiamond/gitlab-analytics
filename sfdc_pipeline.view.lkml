view: sfdc_pipeline {
  derived_table: {
    sql:
        SELECT o.type,
               o.stagename,
               o.closedate,
               o.opportunity_owner__c AS owner,
               o.name,
               SUM(incremental_acv_2__c) AS forecasted_iacv,
               COUNT(*) AS opps
        FROM sfdc.opportunity o
        WHERE closedate >= DATE_TRUNC('month',CURRENT_DATE)
        AND   TYPE!= 'Reseller'
        AND   stagename IN ('0-Pending Acceptance','1-Discovery','2-Scoping',
                            '3-Technical Evaluation','4-Proposal','5-Negotiating',
                            '6-Awaiting Signature')
        AND   (o.isdeleted IS FALSE)
        GROUP BY 1,
                 2,
                 3,
                 4,
                 5
        ;;
  }
  #
  dimension: type {
    description: "Opportunity Type"
    type: string
    sql: ${TABLE}.type ;;
  }
  #
  dimension: stagename {
    description: "Opportunity Stage"
    type: string
    sql: ${TABLE}.stagename ;;
  }
  #
  dimension: owner {
    description: "Opportunity Owner"
    type: string
    drill_fields: [type,stagename,name,closedate_month]
    sql: ${TABLE}.owner ;;
  }
  #
  dimension: name {
    description: "Opportunity Name"
    type: string
    sql: ${TABLE}.name ;;
  }
  dimension_group: closedate {
    description: "The date when an opportunity was closed"
    label: "Opportunity Close Date"
    type: time
    convert_tz: no
    timeframes: [date, week, month, year]
    sql: ${TABLE}.closedate ;;
  }
  #
  measure: forecasted_iacv {
    description: "Opportunity IACV"
    type: sum
    value_format: "$#,##0"
    drill_fields: [type,stagename,owner,name,closedate_month]
    sql: ${TABLE}.forecasted_iacv ;;
  }
  #
  measure: opps {
    description: "Opportunities"
    type: sum
    value_format: "#,##0"
    sql: ${TABLE}.opps ;;
  }

}
