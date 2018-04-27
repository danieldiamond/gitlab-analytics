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

  measure: percentile50_issue_count {
    type: percentile
    percentile: 50
    sql: (${TABLE}.stats->'issues')::text::numeric ;;
  }

  measure: percentile75_issue_count {
    type: percentile
    percentile: 75
    sql: (${TABLE}.stats->'issues')::text::numeric ;;
  }

  measure: percentile90_issue_count {
    type: percentile
    percentile: 90
    sql: (${TABLE}.stats->'issues')::text::numeric ;;
  }

  measure: percentile95_issue_count {
    type: percentile
    percentile: 95
    sql: (${TABLE}.stats->'issues')::text::numeric ;;
  }

  measure: percentile98_issue_count {
    type: percentile
    percentile: 98
    sql: (${TABLE}.stats->'issues')::text::numeric ;;
  }

  measure: percentile99_issue_count {
    type: percentile
    percentile: 99
    sql: (${TABLE}.stats->'issues')::text::numeric ;;
  }

  measure: distinct_uuid_count {
    type: count_distinct
    sql: ${TABLE}.uuid ;;
  }
}
