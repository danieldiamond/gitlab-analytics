view: f_opportunity {
  sql_table_name: analytics.f_opportunity ;;

  dimension: account_id {
    description: "this is the foreign key to dim_account"
    hidden: yes
    type: number
    sql: ${TABLE}.account_id ;;
  }

  dimension: acv {
    type: number
    sql: ${TABLE}.acv ;;
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  dimension: iacv {
    type: number
    sql: ${TABLE}.iacv ;;
  }

  dimension: lead_source_id {
    hidden: yes
    type: number
    sql: ${TABLE}.lead_source_id ;;
  }

  dimension_group: closedate {
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
    sql: ${TABLE}.opportunity_closedate ;;
  }

  dimension: opportunity_id {
    type: string
    sql: ${TABLE}.opportunity_id ;;
  }

  dimension: opportunity_name {
    type: string
    sql: ${TABLE}.opportunity_name ;;

    link: {
      label: "Salesforce Opportunity"
      url: "https://na34.salesforce.com/{{ f_opportunity.opportunity_id._value }}"
      icon_url: "https://c1.sfdcstatic.com/etc/designs/sfdc-www/en_us/favicon.ico"
    }

  }

  dimension: opportunity_product {
    type: string
    sql: ${TABLE}.opportunity_product ;;
  }

  dimension: opportunity_sales_segmentation {
    type: string
    sql: ${TABLE}.opportunity_sales_segmentation ;;
  }

  dimension: opportunity_stage_id {
    hidden: yes
    type: number
    sql: ${TABLE}.opportunity_stage_id ;;
  }

  dimension: opportunity_type {
    type: string
    sql: ${TABLE}.opportunity_type ;;
  }

  dimension: quantity {
    type: number
    sql: ${TABLE}.quantity ;;
  }

  dimension: renewal_acv {
    type: number
    sql: ${TABLE}.renewal_acv ;;
  }

  dimension_group: sales_accepted {
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

  dimension_group: sales_qualified {
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
    sql: ${TABLE}.sales_qualified_date ;;
  }

  dimension: sales_qualified_source {
    type: string
    sql: ${TABLE}.sales_qualified_source ;;
  }

  dimension: tcv {
    type: number
    sql: ${TABLE}.tcv ;;
  }

  measure: number_of_opportunities {
    type: count
    drill_fields: [detail*]
  }

  measure: total_tcv {
    type: sum
    sql: ${tcv} ;;
    drill_fields: [detail*]
    value_format_name: usd
  }

  measure: total_acv {
    type: sum
    sql: ${acv} ;;
    drill_fields: [detail*]
    value_format_name: usd
    }

  measure: total_iacv {
    type: sum
    sql: ${iacv} ;;
    drill_fields: [detail*]
    value_format_name: usd
    }

  set: detail {
    fields: [
      opportunity_name, opportunity_sales_segmentation, total_iacv
    ]
  }
}
