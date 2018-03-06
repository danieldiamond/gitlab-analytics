view: f_opportunity {
  sql_table_name: analytics.f_opportunity ;;

  dimension: account_id {
    description: "This is the foreign key to dim_account"
    hidden: yes
    type: number
    sql: ${TABLE}.account_id ;;
  }

  dimension: acv {
    hidden: yes
    type: number
    sql: ${TABLE}.acv ;;
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }

  dimension: iacv {
    type: number
    hidden: yes
    sql: ${TABLE}.iacv ;;
  }

  dimension: lead_source_id {
    hidden: yes
    type: number
    sql: ${TABLE}.lead_source_id ;;
  }

  dimension_group: closedate {
    label: "Opportunity Close"
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
    label: "SFDC Opportunity ID"
    description: "The 18 char SFDC Opportunity ID"
    type: string
    sql: ${TABLE}.opportunity_id ;;
  }

  dimension: opportunity_name {
    label: "SFDC Opportunity Name"
    description: "The name of the opportunity record from Salesforce."
    type: string
    sql: ${TABLE}.opportunity_name ;;

    link: {
      label: "Salesforce Opportunity"
      url: "https://na34.salesforce.com/{{ f_opportunity.opportunity_id._value }}"
      icon_url: "https://c1.sfdcstatic.com/etc/designs/sfdc-www/en_us/favicon.ico"
    }

  }

  dimension: opportunity_product {
    label: "Product Name"
    type: string
    sql: ${TABLE}.opportunity_product ;;
  }

  dimension: opportunity_sales_segmentation {
    label: "Opportunity Sales Segmentation"
    type: string
    sql: ${TABLE}.opportunity_sales_segmentation ;;
  }

  dimension: opportunity_stage_id {
    description: "The foreign key to dim_opportunitystage"
    hidden: yes
    type: number
    sql: ${TABLE}.opportunity_stage_id ;;
  }

  dimension: opportunity_type {
    label: "Opportunity Type"
    description: "The SFDC opportunity type (New, Renewal,Add-On Business)"
    type: string
    sql: ${TABLE}.opportunity_type ;;
  }

  dimension: quantity {
    label: "Product Quantity"
    type: number
    sql: ${TABLE}.quantity ;;
  }

  dimension: renewal_acv {
    hidden: yes
    type: number
    sql: ${TABLE}.renewal_acv ;;
  }

  dimension_group: sales_accepted {
    label: "Sales Accepted"
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
    label: "Sales Qualified"
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
    description: "Sales Qualified Source"
    type: string
    sql: ${TABLE}.sales_qualified_source ;;
  }

  dimension: tcv {
    label: "Total Contract Value"
    hidden: yes
    type: number
    sql: ${TABLE}.tcv ;;
  }

  measure: number_of_opportunities {
    label: "Count of Opportunities"
    type: count
    drill_fields: [detail*]
  }

  measure: total_tcv {
    label: "Total Contract Value (TCV)"
    type: sum
    sql: ${tcv} ;;
    drill_fields: [detail*]
    value_format_name: usd
  }

  measure: total_acv {
    label: "Total Annual Contract Value (ACV)"
    type: sum
    sql: ${acv} ;;
    drill_fields: [detail*]
    value_format_name: usd
    }

  measure: total_iacv {
    label: "Total Incremental Annual Contract Value (IACV)"
    type: sum
    sql: ${iacv} ;;
    drill_fields: [detail*]
    value_format_name: usd
    }

  measure: total_sqos {
    label: "Total Sales Qualified Opportunities (SQOs)"
    type: count_distinct
    sql:  ${opportunity_id} ;;
    filters: {
      field: dim_leadsource.initial_source
      value: "-Web Direct"
    }
    filters: {
      field: iacv
      value: ">0"
    }
    filters: {
      field: opportunity_type
      value: "New Business,Add-On Business"
    }
    filters: {
      field: sales_qualified_date
      value: "-NULL"
    }
    drill_fields: [detail*]
  }

  set: detail {
    fields: [
      dim_account.name, opportunity_name, opportunity_sales_segmentation, opportunity_type, closedate_date, total_iacv, total_acv
    ]
  }
}
