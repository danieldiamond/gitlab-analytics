view: pipeline_change {
  derived_table: {
    sql: WITH NEW AS
        (SELECT *
         FROM f_snapshot_opportunity
         WHERE date(snapshot_date) = {% date_end date_range %}
          and opportunity_type <> 'Renewal'),

         OLD AS
        (SELECT *
         FROM f_snapshot_opportunity
         WHERE date(snapshot_date) = {% date_start date_range %}
         and opportunity_type <> 'Renewal')

      SELECT 'Starting' as category,
            '1' as order,
             OLD.*
      FROM OLD
      INNER JOIN dim_opportunitystage s ON OLD.opportunity_stage_id=s.id
      WHERE {% condition close_date %} opportunity_closedate {% endcondition %}
      and opportunity_type <> 'Renewal'
      AND sales_accepted_date is not null
      AND s.isclosed=FALSE

      UNION ALL

      SELECT 'Created' as category ,
            '2' as order,
             NEW.*
      FROM NEW
      WHERE NEW.sales_accepted_date > {% date_start date_range %}
        AND NEW.sales_accepted_date <= {% date_end date_range %}
        AND {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        and NEW.opportunity_type <> 'Renewal'

        UNION ALL

      SELECT 'Moved In' as category,
            '3' as order,
            NEW.*
       FROM NEW
       FULL JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
       WHERE ( OLD.opportunity_closedate < {% date_start close_date %}
        OR OLD.opportunity_closedate >= {% date_end close_date %} )
        AND OLD.sales_accepted_date is not null
         and NEW.opportunity_type <> 'Renewal'
      AND {% condition close_date %} NEW.opportunity_closedate {% endcondition %}

        UNION ALL

      SELECT 'Increased' as category, '4' as order, NEW.*
       FROM NEW
       FULL JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
       WHERE ( OLD.opportunity_closedate >= {% date_start close_date %}
       AND OLD.opportunity_closedate < {% date_end close_date %})
        AND OLD.sales_accepted_date is not null
         and NEW.opportunity_type <> 'Renewal'
        AND {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        AND NEW.iacv > old.iacv

         UNION ALL

      SELECT 'Decreased' as category, '6' as order, NEW.*
       FROM NEW
       FULL JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
       WHERE ( OLD.opportunity_closedate >= {% date_start close_date %}
       AND OLD.opportunity_closedate < {% date_end close_date %})
        AND OLD.sales_accepted_date is not null
         and NEW.opportunity_type <> 'Renewal'
        AND {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        AND NEW.iacv < old.iacv

          UNION ALL

      SELECT 'Moved Out' as category, '5' as order, NEW.*
       FROM NEW
       FULL JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
       WHERE ( NEW.opportunity_closedate < {% date_start close_date %}
        OR NEW.opportunity_closedate >= {% date_end close_date %})
        AND OLD.sales_accepted_date is not null
         and NEW.opportunity_type <> 'Renewal'
        AND {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        UNION ALL

        SELECT 'Won' as category, '7' as order, NEW.*
       FROM NEW
       INNER JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
       INNER JOIN dim_opportunitystage s on new.opportunity_stage_id=s.id
       INNER JOIN dim_opportunitystage os on old.opportunity_stage_id=os.id
       WHERE {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
         and NEW.opportunity_type <> 'Renewal'
      AND s.IsWon=true
      AND os.IsClosed=false

        UNION ALL

        SELECT 'Lost' as category, '8' as order, NEW.*
       FROM NEW
       INNER JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
       INNER JOIN dim_opportunitystage s on new.opportunity_stage_id=s.id
       INNER JOIN dim_opportunitystage os on old.opportunity_stage_id=os.id
       WHERE {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
         and NEW.opportunity_type <> 'Renewal'
      AND s.IsWon=false
      AND os.IsClosed=true

      UNION ALL

       SELECT 'Ending' as category, '9' as order,
           New.*
      FROM NEW
      INNER JOIN dim_opportunitystage s ON NEW.opportunity_stage_id = s.id
      AND opportunity_closedate >= {% date_start close_date %}
      AND opportunity_closedate < {% date_end close_date %}
      and opportunity_type <> 'Renewal'
      AND sales_accepted_date is not null
      AND s.isclosed=FALSE
       ;;
  }

  filter: date_range {
    convert_tz: no
    type: date
  }

  filter: close_date {
    convert_tz: no
    type: date
  }


  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
    order_by_field: order
  }

  dimension: order {
    type: number
    sql: ${TABLE}.order ;;
  }

  dimension: opportunity_id {
    type: string
    sql: ${TABLE}.opportunity_id ;;
  }

  dimension_group: snapshot {
    type: time
    timeframes: [raw, date]
    sql: ${TABLE}.snapshot_date ;;
  }

  dimension: account_id {
    type: number
    sql: ${TABLE}.account_id ;;
  }

  dimension: opportunity_stage_id {
    type: number
    sql: ${TABLE}.opportunity_stage_id ;;
  }

  dimension: lead_source_id {
    type: number
    sql: ${TABLE}.lead_source_id ;;
  }

  dimension: opportunity_type {
    type: string
    sql: ${TABLE}.opportunity_type ;;
  }

  dimension: opportunity_sales_segmentation {
    type: string
    sql: ${TABLE}.opportunity_sales_segmentation ;;
  }

  dimension_group: sales_qualified {
    type: time
    timeframes: [raw, date]
    sql: ${TABLE}.sales_qualified_date ;;
  }

  dimension_group: sales_accepted {
    type: time
    timeframes: [raw, date]
    sql: ${TABLE}.sales_accepted_date ;;
  }

  dimension: sales_qualified_source {
    type: string
    sql: ${TABLE}.sales_qualified_source ;;
  }

  dimension_group: opportunity_close {
    type: time
    timeframes: [raw, date]
    sql: ${TABLE}.opportunity_closedate ;;
  }

  dimension: opportunity_name {
    type: string
    sql: ${TABLE}.opportunity_name ;;
  }

  dimension: iacv {
    hidden: yes
    type: number
    sql: ${TABLE}.iacv ;;
  }

  dimension: renewal_acv {
    hidden: yes
    type: number
    sql: ${TABLE}.renewal_acv ;;
  }

  dimension: acv {
    hidden: yes
    type: number
    sql: ${TABLE}.acv ;;
  }

  dimension: tcv {
    hidden: yes
    type: number
    sql: ${TABLE}.tcv ;;
  }




  parameter: metric_type {
    description: "Choose which ACV type to measure"
    allowed_value: { value: "ACV" }
    allowed_value: { value: "IACV" }
    allowed_value: { value: "Renewal ACV" }
    allowed_value: { value: "TCV" }
    default_value: "IACV"

  }

  measure: acv_metric {
    label: "ACV Metric"
    description: "use the metric type filter to choose which ACV to measure"
    type: number
    sql: CASE
          WHEN {% parameter metric_type %} = 'ACV' THEN ${total_acv}
          WHEN {% parameter metric_type %} = 'IACV' THEN ${total_iacv}
          WHEN {% parameter metric_type %} = 'Renewal ACV' THEN ${total_renewal_acv}
          WHEN {% parameter metric_type %} = 'TCV' THEN ${total_tcv}
        END ;;
    label_from_parameter: metric_type
    value_format_name: usd
    drill_fields: [detail*]
  }


  measure: total_acv {
    hidden: yes
    label: "Total ACV"
    type: sum
    sql: CASE
          WHEN ${category} IN ('Starting', 'Created', 'Moved In', 'Increased', 'Ending') THEN ${acv}
          WHEN ${category} IN ('Moved Out', 'Decreased', 'Won', 'Lost') THEN -1.0*${acv}
        END ;;
    value_format_name: usd
  }

  measure: total_iacv {
    hidden: yes
    label: "Total IACV"
    type: sum
    sql: CASE
          WHEN ${category} IN ('Starting', 'Created', 'Moved In', 'Increased', 'Ending') THEN ${iacv}
          WHEN ${category} IN ('Moved Out', 'Decreased', 'Won', 'Lost') THEN -1.0*${iacv}
        END ;;
    value_format_name: usd
  }

  measure: total_renewal_acv {
    hidden: yes
    label: "Total Renewal ACV"
    type: sum
    sql: CASE
          WHEN ${category} IN ('Starting', 'Created', 'Moved In', 'Increased', 'Ending') THEN ${renewal_acv}
          WHEN ${category} IN ('Moved Out', 'Decreased', 'Won', 'Lost') THEN -1.0*${renewal_acv}
        END ;;
    value_format_name: usd
  }

  measure: total_tcv {
    hidden: yes
    label: "Total TCV"
    type: sum
    sql: CASE
          WHEN ${category} IN ('Starting', 'Created', 'Moved In', 'Increased', 'Ending') THEN ${tcv}
          WHEN ${category} IN ('Moved Out', 'Decreased', 'Won', 'Lost') THEN -1.0*${tcv}
        END ;;
    value_format_name: usd
  }





  set: detail {
    fields: [
    ]
  }
}
