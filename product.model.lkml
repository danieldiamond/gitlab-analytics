connection: "production_dw"

include: "usage_data.view.lkml"
label: "product"

explore: usage_data {
  label: "Usage Data"
  description: "Usage Data"
}
