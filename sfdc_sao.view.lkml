view: sfdc_sao {
  derived_table: {
    sql:
        SELECT 'Mid-Market' AS opp_segment,
               a.name AS account_name,
               o.name AS opp_name,
               o.sales_accepted_date__c AS sales_accepted_date,
               o.sql_source__c AS gen_type,
               COALESCE(o.sales_segmentation_o__c,'Unknown') AS sales_segment,
               ultimate_parent_sales_segment_o__c AS parent_segment,
               stagename,
               o.leadsource,
               o.type,
               o.amount AS tcv,
               o.incremental_acv__c AS iacv
        FROM sfdc.opportunity o
          LEFT JOIN sfdc.account a ON a.id = o.accountid
        WHERE (o.TYPE IN ('New Business','Add-On Business') OR o.TYPE IS NULL)
        AND   (o.sales_segmentation_o__c = 'Mid-Market' OR o.ultimate_parent_sales_segment_o__c = 'Mid-Market')
        AND   o.sales_segmentation_o__c NOT IN ('Large','Strategic')
        AND   (o.ultimate_parent_sales_segment_o__c NOT IN ('Large','Strategic') OR o.ultimate_parent_sales_segment_o__c IS NULL)
        AND   (stagename NOT IN ('00-Pre Opportunity','9-Unqualified','10-Duplicate'))
        AND   amount >= 0
        AND   (o.isdeleted IS FALSE)
        AND   o.leadsource != 'Web Direct'

        UNION ALL

        SELECT CASE WHEN (sales_segmentation_o__c = 'Large' OR ultimate_parent_sales_segment_o__c = 'Large') THEN 'Large'
                ELSE 'Strategic'
               END AS opp_segment,
               a.name AS account_name,
               o.name AS opp_name,
               o.sales_accepted_date__c AS sales_accepted_date,
               o.sql_source__c AS gen_type,
               COALESCE(o.sales_segmentation_o__c,'Unknown') AS sales_segment,
               ultimate_parent_sales_segment_o__c AS parent_segment,
               stagename,
               o.leadsource,
               o.type,
               o.amount AS tcv,
               o.incremental_acv__c AS iacv
        FROM sfdc.opportunity o
          LEFT JOIN sfdc.account a ON a.id = o.accountid
        WHERE (o.TYPE IN ('New Business','Add-On Business') OR o.TYPE IS NULL)
        AND   (o.sales_segmentation_o__c IN ('Large','Strategic') OR o.ultimate_parent_sales_segment_o__c IN ('Large','Strategic'))
        AND   (stagename NOT IN ('00-Pre Opportunity','9-Unqualified','10-Duplicate'))
        AND   amount >= 0
        AND   (o.isdeleted IS FALSE)
        AND   o.leadsource != 'Web Direct'

        ORDER BY sales_accepted_date DESC;;
  }
  #
  dimension: opp_segment {
    description: "Opp Segment"
    type: string
    sql: ${TABLE}.opp_segment ;;
  }
  dimension: customer {
    description: "Customer"
    type: string
    sql: ${TABLE}.account_name ;;
  }
  #
  dimension: opportunity {
    description: "Opp Name"
    type: string
    sql: ${TABLE}.opp_name ;;
  }
  #
  dimension: acct_num {
    description: "Acct # of Customer"
    type: string
    sql: ${TABLE}.accountnumber ;;
  }
  #
  dimension: lead_type {
    description: "Lead Type"
    type: string
    sql: ${TABLE}.gen_type ;;
  }
  #
  dimension: sales_segment {
    description: "Sales Segment"
    type: string
    sql: ${TABLE}.sales_segment ;;
  }
  #
  dimension: parent_segment {
    description: "Parent Segment"
    type: string
    sql: ${TABLE}.parent_segment ;;
  }
  #
  dimension: stagename {
    description: "Stage Opp Is In"
    type: string
    sql: ${TABLE}.stagename ;;
  }
  #
  dimension: leadsource {
    description: "Source of Lead"
    type: string
    sql: ${TABLE}.leadsource ;;
  }
  #
  dimension: type {
    description: "Deal Type"
    type: string
    sql: ${TABLE}.type ;;
  }
  #
  dimension_group: sales_accepted_date {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.sales_accepted_date ;;
  }

  #
  measure: tcv {
    description: "TCV"
    type: sum
    sql: ${TABLE}.tcv ;;
  }
  #
  measure: iacv {
    description: "IACV"
    type: sum
    sql: ${TABLE}.iacv ;;
  }
  #
  measure: segment_cnt {
    description: "Count from Opp Segment"
    type: count_distinct
    sql: ${TABLE}.opp_name ;;
  }

}
