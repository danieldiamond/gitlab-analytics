connection: "production_dw"

include: "*.view.lkml"         # include all views in this project
include: "*.dashboard.lookml"  # include all dashboards in this project



explore: f_opportunity {
  label: "Sales"
  description: "Start here for questions around Sales data"

  view_label: "Opportunity"

  join: dim_account {
    view_label: "Account"
    type: inner
    relationship: many_to_one
    sql_on: ${f_opportunity.account_id} = ${dim_account.id} ;;
  }

  join: dim_leadsource {
    view_label: "Lead Source"
    type: inner
    relationship: many_to_one
    sql_on: ${f_opportunity.lead_source_id} = ${dim_leadsource.id} ;;
  }

  join: dim_opportunitystage {
    view_label: "Opportunity Stage"
    type: inner
    relationship: many_to_one
    sql_on: ${f_opportunity.opportunity_stage_id} = ${dim_opportunitystage.id} ;;
  }
}


explore: pipeline_change {
  label: "Sales Pipeline Change"
  description: "Use this explore to look at the change in pipeline over time"

  always_filter: {
    filters: {
      field: close_date
      value: "this month"
    }

    filters: {
      field: date_range
      value: "7 days ago for 7 days"
    }

    filters: {
      field: metric_type
      value: "IACV"
    }
  }

}

explore: f_churn_history {
  label: "Account Churn History"
  description: "Use this explore to look at subscription churn"

  join: dim_account {
    view_label: "Account"
    type: inner
    relationship: many_to_one
    sql_on: ${f_churn_history.id} = ${dim_account.sfdc_account_id} ;;
  }
}
