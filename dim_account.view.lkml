view: dim_account {
  sql_table_name: analytics.dim_account ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: industry {
    type: string
    sql: ${TABLE}.industry ;;
  }

  dimension: is_lau {
    type: yesno
    sql: ${TABLE}.is_lau ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: sales_segmentation {
    type: string
    sql: ${TABLE}.sales_segmentation ;;
  }

  dimension: sfdc_account_id {
    type: string
    sql: ${TABLE}.sfdc_account_id ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: ultimate_parent_name {
    type: string
    sql: ${TABLE}.ultimate_parent_name ;;
  }

  dimension: ultimate_parent_sales_segmentation {
    type: string
    sql: ${TABLE}.ultimate_parent_sales_segmentation ;;
  }

  measure: count {
    type: count
    drill_fields: [id, ultimate_parent_name, name]
  }
}
