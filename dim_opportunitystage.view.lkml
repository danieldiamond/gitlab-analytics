view: dim_opportunitystage {
  sql_table_name: analytics.dim_opportunitystage ;;

  dimension: id {
    primary_key: yes
    hidden: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: defaultprobability {
    label: "Default Probability"
    type: number
    sql: ${TABLE}.defaultprobability ;;
  }

  dimension: isactive {
    label: "Is Active"
    type: yesno
    sql: ${TABLE}.isactive ;;
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
}
