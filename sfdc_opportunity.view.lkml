view: sfdc_opportunity {
  sql_table_name: sfdc.opportunity;;
  # # Define your dimensions and measures here, like this:
  #
  dimension: id {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${TABLE}.id ;;
  }
  #
  dimension: isdeleted {
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
    description: "total contract value"
    type: sum
    sql: ${TABLE}.amount ;;
  }
#
  measure: renewal_amt {
    description: "Renewal Amount"
    type: sum
    sql: ${TABLE}.renewal_amount__c ;;
  }
#
  measure: renewal_acv {
    description: "Renewal ACV"
    type: sum
    sql: ${TABLE}.renewal_acv__c ;;
  }
#
  measure: acv {
    description: "Annual contract value"
    type: sum
    sql: ${TABLE}.acv__c ;;
  }
#
  measure: iacv {
    description: "Incremental annual contract value"
    type: sum
    sql: ${TABLE}.incremental_acv__c ;;
  }
#
  measure: nrv {
    description: "Non recurring value (ex:proserv, training)"
    type: sum
    sql: ${TABLE}.nrv__c ;;
  }

}
