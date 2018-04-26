view: usage_data {
  sql_table_name: version.usage_data ;;
  label: "Usage Data"

  dimension: created_at_month {
    type: date_month
    sql: ${TABLE}.created_at ;;
  }

  dimension: uuid {
    type: string
    sql: ${TABLE}.uuid ;;
  }

  measure: distinct_uuid_count {
    type: count_distinct
    sql: ${TABLE}.uuid ;;
  }
}
