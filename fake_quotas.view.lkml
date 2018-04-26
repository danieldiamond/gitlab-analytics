view: fake_quotas {
  sql_table_name: sandbox.fake_quotas ;;

  dimension: booked {
    type: number
    sql: ${TABLE}.booked ;;
  }

  dimension: goal {
    type: number
    sql: ${TABLE}.goal ;;
  }

  dimension: month_date {
    type: string
    sql: ${TABLE}.month_date;;
  }

#   dimension_group: date {
#     type: time
#     timeframes: [raw, month, year]
#     sql: ${TABLE}.month_date ;;
#   }

  dimension: opp_owner {
    type: string
    sql: ${TABLE}.opp_owner ;;
  }

  measure: percentage {
    type: number
    sql: 1.0*${total_booked} / nullif(${total_goal},0) ;;
    value_format_name: percent_1
  }

  measure: total_booked {
    type: number
    sql:  sum(${booked}) ;;
  }

  measure: total_goal {
    type: number
    sql:  sum(${goal}) ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
