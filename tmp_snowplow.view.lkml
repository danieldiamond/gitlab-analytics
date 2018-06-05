view: events{
  sql_table_name: public.fake_events ;;

  dimension: event_id {
    type: string
    sql: ${TABLE}.event_id ;;
  }
  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }
  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }
  dimension_group: event_time {
    type: time
    timeframes: [time, date, week, month, year]
    sql: ${TABLE}.event_time ;;
  }
  dimension: browser {
    type: string
    sql: ${TABLE}.browser ;;
  }
  dimension: operating_system {
    type: string
    sql: ${TABLE}.operating_system ;;
  }
  measure: event_count {
    label: "Number of Events"
    type: count_distinct
    sql: ${event_id} ;;
  }
  measure: user_count {
    label: "Number of Users"
    type: count_distinct
    sql: ${user_id} ;;
  }
  measure: booking_created {
    label: "Booking Created"
    type: count
    filters: {
      field: event
      value: "Booking Created"
    }
    value_format_name: decimal_0
  }
  measure: booking_completed {
    label: "Booking Completed"
    type: count
    filters: {
      field: event
      value: "Booking Completed"
    }
    value_format_name: decimal_0
  }
  measure: booking_paid {
    label: "Booking Paid"
    type: count
    filters: {
      field: event
      value: "Booking Paid"
    }
    value_format_name: decimal_0
  }
  measure: finish_booking_rate {
    type: number
    sql: ${booking_completed}::float / nullif(${booking_created}, 0) ;;
    value_format_name: percent_1
  }
  measure: pay_booking_rate {
    type: number
    sql: ${booking_paid}::float / nullif(${booking_completed}, 0) ;;
    value_format_name: percent_1
  }
}
