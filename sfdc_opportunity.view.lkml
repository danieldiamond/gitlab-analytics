view: sfdc_opportunity {
  sql_table_name: sfdc.opportunity;;
  # # Define your dimensions and measures here, like this:
  dimension: id {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${TABLE}.id ;;
  }
  #
  dimension: isdeleted {
    label: "Is Deleted"
    description: "Filter out corrupt data"
    type: yesno
    sql: ${TABLE}.isdeleted ;;
  }
  #
  dimension: subscription_type {
    description: "Sale type based on subscription"
    type: string
    sql: ${TABLE}.TYPE ;;
  }
  #
  dimension: sale_stage {
    description: "Stage in which a sale is in"
    type: string
    sql: ${TABLE}.stagename ;;
  }
  #
  dimension_group: closedate {
    description: "The date when an opportunity was closed"
    label: "Close"
    type: time
    convert_tz: no
    timeframes: [date, week, month, year]
    sql: ${TABLE}.closedate ;;
  }
  #
  dimension: sale_type {
    description: "Sales assisted or web direct sale"
    type: string
    sql: ${TABLE}.engagement_type__c ;;
  }
  #
  dimension: products {
    description: "Product that is tied to opportunity"
    type: string
    sql: ${TABLE}.products_purchased__c ;;
  }
#
  measure: tcv {
    label: "TCV - Total Contract Value"
    type: sum
    sql: ${TABLE}.amount ;;
    value_format: "$#,##0"
  }
#
  measure: renewal_amt {
    description: "Renewal Amount"
    type: sum
    sql: ${TABLE}.renewal_amount__c ;;
    value_format: "$#,##0"
  }
#
  measure: renewal_acv {
    label: "Renewal ACV"
    type: sum
    sql: ${TABLE}.renewal_acv__c ;;
    value_format: "$#,##0"
  }
#
  measure: acv {
    label: "ACV - Annual contract value"
    type: sum
    sql: ${TABLE}.acv__c ;;
    value_format: "$#,##0"
  }
#
  measure: iacv {
    label: "IACV - Incremental annual contract value"
    type: sum
    sql: ${TABLE}.incremental_acv__c ;;
    value_format: "$#,##0"
  }
#
  measure: nrv {
    label: "NRV - Non Recurring Value"
    description: "Example: proserv, training, etc."
    type: sum
    sql: ${TABLE}.nrv__c ;;
    value_format: "$#,##0"
  }

}
