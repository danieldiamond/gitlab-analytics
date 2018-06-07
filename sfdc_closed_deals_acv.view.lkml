view: sfdc_closed_deals_acv {
  derived_table: {
    sql:
        WITH acv_data AS
            (
              SELECT o.type AS sales_type,
                     o.closedate,
                     o.id,
                     o.opportunity_owner__c AS OWNER,
                     o.engagement_type__c AS sales_path,
                     o.acv_2__c AS acv,
                     a.sales_segmentation__c AS segment,
                     /*
                       We dont want to include closed deals that
                       that had negative impact
                     */ CASE
                       WHEN o.acv_2__c >= 0 THEN 1
                       ELSE 0
                     END AS closed_deals
              FROM sfdc.opportunity o
                INNER JOIN sfdc.account a ON a.id = o.accountid
              WHERE o.type!= 'Reseller'
              AND   o.stagename IN ('Closed Won')
              AND   (o.isdeleted IS FALSE)
            )

            SELECT owner, --
                   sales_path, --
                   sales_type, --
                   segment, --
                   closedate,
                   SUM(acv) AS acv,
                   SUM(closed_deals) AS closed_deals
            FROM acv_data
            GROUP BY 1,
                     2,
                     3,
                     4,
                     5
        ;;
  }
  #
  dimension: sales_type {
    description: "Sales Type"
    type: string
    sql: ${TABLE}.sales_type ;;
  }
  #
  dimension: sales_path {
    description: "Sales Path"
    type: string
    sql: ${TABLE}.sales_path ;;
  }
  #
  dimension: owner {
    description: "Opportunity Owner"
    type: string
    drill_fields: [sales_type,sales_path,segment,closedate_month]
    sql: ${TABLE}.owner ;;
  }
  #
  dimension: segment {
    description: "Segment"
    type: string
    sql: ${TABLE}.segment ;;
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
  measure: acv {
    description: "Closed Deal ACV"
    type: sum
    value_format: "$#,##0"
    drill_fields: [sales_type,sales_path,segment,owner,closedate_month]
    sql: ${TABLE}.acv ;;
  }
  #
  measure: closed_deals {
    description: "Closed Deals"
    type: sum
    value_format: "#,##0"
    sql: ${TABLE}.closed_deals ;;
  }

}
