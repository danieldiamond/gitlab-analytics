connection: "production_dw"

include: "usage_data.view.lkml"

explore: usage_data {
  group_label: "product"
  label: "Usage Data"
  description: "Usage Data"
}
