view: historical_sales_quota {
  sql_table_name: analytics.historical_sales_quota ;;

  dimension: account_owner {
    type: string
    sql: ${TABLE}.account_owner ;;
  }


  dimension: adjusted_start_date {
    type: string
    sql: ${TABLE}.adjusted_start_date ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
