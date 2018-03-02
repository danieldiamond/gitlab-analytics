view: dim_leadsource {
  sql_table_name: analytics.dim_leadsource ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: initial_source {
    type: string
    sql: ${TABLE}.initial_source ;;
  }

  dimension: initial_source_type {
    type: string
    sql: ${TABLE}.initial_source_type ;;
  }

  measure: count {
    type: count
    drill_fields: [id]
  }
}
