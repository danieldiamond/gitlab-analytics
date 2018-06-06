connection: "production_dw"

include: "*.view.lkml"         # include all views in this project
#include: "*.dashboard.lookml"  # include all dashboards in this project
label: "finance"

explore: bookings {
  from: sfdc_opportunity
  label: "Bookings"
  description: "Bookings Metrics (ex: TCV, IACV)"
  always_filter: {
    filters: {
      field: isdeleted
      value: "No"
    }
    filters: {
      field: sale_stage
      value: "Closed Won"
    }
    filters: {
      field: subscription_type
      value: "-Reseller"
    }
  }

}
#
explore: invoicing {
  view_label: "Invoicing"
  from:  zuora_ar
  label: "A/R Aging"
  description: "A/R Oustanding"

  join: beyond_90days_open_invoices {
  fields: []
  view_label: "Invoicing"
  sql_on:     ${invoicing.customer} = ${beyond_90days_open_invoices.name}
          AND ${invoicing.day_range} = ${beyond_90days_open_invoices.day_range};;
  relationship: many_to_one
  }
}

explore: customers_and_arr {
  from: zuora_current_arr
  label: "Current ARR & Customers"
}

explore: pipeline {
  from: sfdc_pipeline
  label: "Sales Pipeline"
  description: "Pipeline Opportunities"
}
