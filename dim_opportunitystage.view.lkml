view: dim_opportunitystage {
  sql_table_name: analytics.dim_opportunitystage ;;

  dimension: id {
    primary_key: yes
    hidden: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: apiname {
    type: string
    sql: ${TABLE}.apiname ;;
  }

  dimension: createdbyid {
    hidden: yes
    type: string
    sql: ${TABLE}.createdbyid ;;
  }

  dimension_group: createddate {
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
    type: yesno
    sql: ${TABLE}.isclosed ;;
  }

  dimension: iswon {
    type: yesno
    sql: ${TABLE}.iswon ;;
  }

  dimension: lastmodifiedbyid {
    hidden: yes
    type: string
    sql: ${TABLE}.lastmodifiedbyid ;;
  }

  dimension_group: lastmodifieddate {
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
    type: number
    sql: ${TABLE}.sortorder ;;
  }

  dimension: systemmodstamp {
    hidden: yes
    type: string
    sql: ${TABLE}.systemmodstamp ;;
  }

}
