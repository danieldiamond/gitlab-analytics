connection: "production_dw"

include: "usage_*.view.lkml"
label: "product"

explore: usage_data {
  label: "Usage Data"
  description: "All dimensions are counts of that feature for a given instance."
}
