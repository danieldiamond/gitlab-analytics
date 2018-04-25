view: usage_data {
  sql_table_name: version.usage_data ;;
  label: "Usage Data"

  dimension: hostname {
    label: "hostname"
    type: string
    sql: ${TABLE}.hostname ;;
  }
}
