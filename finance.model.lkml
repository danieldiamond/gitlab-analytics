connection: "production_dw"

include: "*.view.lkml"         # include all views in this project
#include: "*.dashboard.lookml"  # include all dashboards in this project
label: "finance"

# # Select the views that should be a part of this model,
# # and define the joins that connect them together.
#
# explore: order_items {
#   join: orders {
#     relationship: many_to_one
#     sql_on: ${orders.id} = ${order_items.order_id} ;;
#   }
#
#   join: users {
#     relationship: many_to_one
#     sql_on: ${users.id} = ${orders.user_id} ;;
#   }
# }
explore: bookings {
  from: sfdc_opportunity
  label: "Bookings"
  description: "Bookings Metrics (ex: TCV, IACV)"
}
#
explore: zuora_invoice {
  label: "A/R Aging"
  description: "A/R Oustanding"
  join: zuora_account {
    type: inner
    relationship: one_to_one
    sql_on: ${zuora_invoice.accountid} = ${zuora_account.id} ;;

  }
}
