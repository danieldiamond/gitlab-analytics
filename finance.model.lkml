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
explore: invoicing {
  from:  zuora_ar
  label: "A/R Aging"
  description: "A/R Oustanding"
}
