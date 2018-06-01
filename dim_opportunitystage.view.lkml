view: dim_opportunitystage {
  sql_table_name: analytics.dim_opportunitystage ;;

  dimension: id {
    primary_key: yes
    hidden: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: apiname {
    label: "API Name"
    type: string
    sql: ${TABLE}.apiname ;;
  }

  dimension: createdbyid {
    hidden: yes
    type: string
    sql: ${TABLE}.createdbyid ;;
  }

  dimension_group: createddate {
    label: "Created"
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.createddate ;;
  }

  dimension: defaultprobability {
    label: "Default Probability"
    type: number
    sql: ${TABLE}.defaultprobability ;;
  }

  dimension: isclosed {
    label: "Is Closed"
    type: yesno
    sql: ${TABLE}.isclosed ;;
  }

  dimension: iswon {
    label: "Is Won"
    type: yesno
    sql: ${TABLE}.iswon ;;
  }

  dimension: lastmodifiedbyid {
    hidden: yes
    type: string
    sql: ${TABLE}.lastmodifiedbyid ;;
  }

  dimension_group: lastmodifieddate {
    label: "Last Modified"
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.lastmodifieddate ;;
  }

  dimension: masterlabel {
    hidden: yes
    type: string
    sql: ${TABLE}.masterlabel ;;
  }

  dimension: mapped_stage {
    full_suggestions: yes
    label: "Stage Name"
    type: string
    sql: ${TABLE}.mapped_stage ;;
  }

  dimension: sfdc_id {
    hidden: yes
    type: string
    sql: ${TABLE}.sfdc_id ;;
  }

  dimension: sortorder {
    label: "Sort Order"
    type: number
    sql: ${TABLE}.sortorder ;;
  }

  dimension: systemmodstamp {
    hidden: yes
    type: string
    sql: ${TABLE}.systemmodstamp ;;
  }

}
