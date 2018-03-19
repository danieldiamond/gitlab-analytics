- dashboard: pipeline_flow
  title: Pipeline Flow
  layout: newspaper
  elements:
  - title: Pipeline Flow
    name: Pipeline Flow
    model: gitlab
    explore: pipeline_change
    type: looker_column
    fields:
    - pipeline_change.category
    - pipeline_change.total_acv
    filters:
      pipeline_change.close_date: this month
      pipeline_change.date_range: 7 days ago for 7 days
      pipeline_change.metric_type: IACV
    sorts:
    - pipeline_change.category
    limit: 500
    column_limit: 50
    dynamic_fields:
    - table_calculation: offset
      label: offset
      expression: running_total(offset(${starting},-1)+offset(${added},-1)-${subtracted})
      value_format:
      value_format_name: usd_0
      _kind_hint: measure
      _type_hint: number
    - table_calculation: starting
      label: Starting
      expression: if(${pipeline_change.category} = "Starting", ${pipeline_change.total_acv},0)
      value_format:
      value_format_name: usd_0
      _kind_hint: measure
      _type_hint: number
    - table_calculation: added
      label: Added
      expression: |-
        if(${pipeline_change.category} = "Created" OR ${pipeline_change.category} = "Moved In" OR
          ${pipeline_change.category} = "Increased"
          , ${pipeline_change.total_acv},0)
      value_format:
      value_format_name: usd_0
      _kind_hint: measure
      _type_hint: number
    - table_calculation: subtracted
      label: Subtracted
      expression: |-
        if(${pipeline_change.category} = "Moved Out" OR ${pipeline_change.category} = "Decreased" OR
          ${pipeline_change.category} = "Won" OR
          ${pipeline_change.category} = "Lost"
          , -${pipeline_change.total_acv},0)
      value_format:
      value_format_name: usd_0
      _kind_hint: measure
      _type_hint: number
    stacking: normal
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: false
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#707070"
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: editable
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    series_types: {}
    hidden_fields:
    - pipeline_change.total_acv
    series_colors:
      calculation_1: "#d4cceb"
      offset: transparent
      starting: "#00AAEA"
      added: "#0ABB45"
      subtracted: "#FC6250"
    hidden_series: []
    label_color:
    - transparent
    - white
    hide_legend: true
    font_size: ''
    row: 0
    col: 0
    width: 24
    height: 18
