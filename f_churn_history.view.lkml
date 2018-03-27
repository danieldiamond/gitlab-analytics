view: f_churn_history {
  sql_table_name: analytics.f_churn_history ;;

  dimension: id {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: change {
    type: number
    hidden: yes
    sql: ${TABLE}.change ;;
  }

  dimension: retention_type {
    label: "Retention Type"
    case: {
      when: {
        sql: ${current_total} = 0 AND ${year_ago_total} >0 ;;
        label: "Cancellation"
      }
      when: {
        sql: ${current_total} < ${year_ago_total} AND ${current_total} <> 0 ;;
        label: "Downgrade"
      }
      when: {
        sql: ${current_total} > ${year_ago_total} ;;
        label: "Upgrade"
      }
      # possibly more when statements
      else: "Retention"
    }
  }

  dimension_group: curr_end {
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
    sql: ${TABLE}.curr_end_date ;;
  }

  dimension_group: curr_start {
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
    sql: ${TABLE}.curr_start_date ;;
  }

  dimension: current_arr {
    type: number
    sql: ${TABLE}.current_arr ;;
  }

  dimension: current_mrr {
    type: number
    sql: ${TABLE}.current_mrr ;;
  }

  dimension: current_total {
    type: number
    sql: ${TABLE}.current_total ;;
  }

  dimension: current_trueup {
    type: number
    sql: ${TABLE}.current_trueup ;;
  }

  dimension: period {
    type: string
    sql: ${TABLE}.period ;;
  }

  dimension: year_ago_arr {
    type: number
    sql: ${TABLE}.year_ago_arr ;;
  }

  dimension_group: year_ago_end {
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
    sql: ${TABLE}.year_ago_end_date ;;
  }

  dimension: year_ago_mrr {
    type: number
    sql: ${TABLE}.year_ago_mrr ;;
  }

  dimension_group: year_ago_start {
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
    sql: ${TABLE}.year_ago_start_date ;;
  }

  dimension: year_ago_total {
    type: number
    sql: ${TABLE}.year_ago_total ;;
  }

  dimension: year_ago_trueup {
    type: number
    sql: ${TABLE}.year_ago_trueup ;;
  }

  measure: count {
    type: count_distinct
    drill_fields: [id]
  }

  measure: arr_year_ago {
    label: "Year Ago ARR"
    type: sum
    sql: ${year_ago_arr} ;;
  }

  measure: total_year_ago {
    label: "Year Ago Total"
    type: sum
    sql: ${year_ago_total} ;;
  }
  measure: total_current {
    label: "Current Total"
    type: sum
    sql: ${current_total} ;;
  }

  measure: delta {
    label: "Change from Year Ago"
    type: sum
    sql: ${TABLE}.change ;;
  }

  measure: net_retention {
    label: "Net Retention"
    type: number
    value_format: "#.00\%"
    sql: 100 * ${total_current}/NULLIF(${total_year_ago},0) ;;
  }

}
