view: pipeline_change {
  derived_table: {
    sql: WITH NEW AS
        (SELECT *
         FROM analytics.f_snapshot_opportunity a
         INNER JOIN dim_opportunitystage s ON a.opportunity_stage_id=s.id
         WHERE date(snapshot_date) = {% date_end date_range %}
          AND s.mapped_stage != 'Unmapped'),

         OLD AS
        (SELECT *
         FROM analytics.f_snapshot_opportunity a
         INNER JOIN dim_opportunitystage os ON a.opportunity_stage_id=os.id
         WHERE date(snapshot_date) = {% date_start date_range %}
          AND os.mapped_stage != 'Unmapped')

      SELECT
        'Starting' AS category,
        '1' AS order,
        OLD.*
      FROM OLD
      JOIN dim_opportunitystage os ON OLD.opportunity_stage_id=os.id
      WHERE (OLD.sales_accepted_date < {% date_start date_range %}
            OR old.sales_accepted_date ISNULL
            OR old.opportunity_closedate= {% date_start date_range %})
        AND {% condition close_date %} opportunity_closedate {% endcondition %}
        AND os.isclosed=FALSE
        AND os.mapped_stage != '0-Pending Acceptance'

      UNION ALL

      SELECT
        'Created' as category ,
        '2' as order,
        NEW.*
      FROM NEW
      INNER JOIN dim_opportunitystage s ON NEW.opportunity_stage_id=s.id
      WHERE NEW.sales_accepted_date >= {% date_start date_range %}
        AND NEW.sales_accepted_date <= {% date_end date_range %}
        AND {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        AND (
              s.isclosed=false
              OR
              (s.isclosed=true AND s.iswon = true)
            )

      UNION ALL
  -- For opps that show up w/o a sales_accepted_date and aren't in the OLD list
      SELECT
        'Created' As category ,
        '2' AS order,
        NEW.*
      FROM NEW
      FULL JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
      FULL JOIN dim_opportunitystage s ON NEW.opportunity_stage_id=s.id
      FULL JOIN dim_opportunitystage os ON OLD.opportunity_stage_id=os.id
      WHERE {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        AND NEW.sales_accepted_date ISNULL
        AND NEW.opportunity_closedate >= {% date_start date_range %}
        AND
            (
              (OLD.opportunity_name ISNULL AND (os.mapped_stage != '0-Pending Acceptance' OR os.mapped_stage ISNULL) AND s.isclosed=false)
              OR
              (os.mapped_stage = '0-Pending Acceptance' AND s.isclosed=true)
              OR
              (os.mapped_stage= '0-Pending Acceptance' AND s.mapped_stage
                  IN ('1-Discovery','2-Scoping','3-Technical Evaluation','4-Propoasl','5-Negotiating','6-Awaiting Signature'))
              OR
              (OLD.opportunity_name ISNULL and s.isclosed=true AND s.iswon=true)
              OR
              (NEW.opportunity_closedate = {% date_start date_range %} and s.isclosed=true AND s.iswon=true)
            )

      UNION ALL

      SELECT
        'Moved In' AS category,
        '3' AS order,
        NEW.*
      FROM NEW
      FULL JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
      INNER JOIN dim_opportunitystage os ON OLD.opportunity_stage_id=os.id
      WHERE ( OLD.opportunity_closedate < {% date_start close_date %}
        OR OLD.opportunity_closedate >= {% date_end close_date %} )
        AND {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        AND os.mapped_stage != '0-Pending Acceptance'

      UNION ALL

      SELECT
        'Increased' AS category,
        '4' AS order,
        NEW.*
      FROM NEW
      FULL JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
      INNER JOIN dim_opportunitystage os on old.opportunity_stage_id=os.id
      WHERE ( OLD.opportunity_closedate >= {% date_start close_date %}
        AND OLD.opportunity_closedate < {% date_end close_date %})
        AND {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        AND NEW.iacv > old.iacv
        AND os.isclosed=false
        AND os.mapped_stage != '0-Pending Acceptance'

      UNION ALL

      SELECT
        'Decreased' AS category,
        '6' AS order,
        NEW.*
      FROM NEW
      FULL JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
      INNER JOIN dim_opportunitystage os on old.opportunity_stage_id=os.id
      WHERE ( OLD.opportunity_closedate >= {% date_start close_date %}
        AND OLD.opportunity_closedate < {% date_end close_date %})
        AND {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        AND NEW.iacv < old.iacv
        AND os.isclosed=false
        AND os.mapped_stage != '0-Pending Acceptance'

      UNION ALL

      SELECT
        'Moved Out' AS category,
        '5' AS order,
        NEW.*
      FROM NEW
      FULL JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
      INNER JOIN dim_opportunitystage os ON old.opportunity_stage_id=os.id
      WHERE ( NEW.opportunity_closedate < {% date_start close_date %}
        OR NEW.opportunity_closedate >= {% date_end close_date %})
        AND {% condition close_date %} OLD.opportunity_closedate {% endcondition %}
        AND os.isclosed=false
        AND os.mapped_stage != '0-Pending Acceptance'

      UNION ALL

      SELECT
          'Won' AS category,
          '7' AS order,
          NEW.*
      FROM NEW
      FULL JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
      FULL JOIN dim_opportunitystage s ON new.opportunity_stage_id=s.id
      FULL JOIN dim_opportunitystage os ON old.opportunity_stage_id=os.id
      WHERE {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        AND s.IsWon=true
        AND (os.IsClosed=false OR NEW.opportunity_closedate = {% date_start date_range %})
        AND NEW.opportunity_closedate >= {% date_start date_range %}

      UNION ALL
        -- This is for opps that are won but don't show up in the old list
      SELECT
        'Won' AS category,
        '7' AS order,
        NEW.*
      FROM NEW
      FULL JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
      INNER JOIN dim_opportunitystage s ON new.opportunity_stage_id=s.id
      WHERE {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        AND s.iswon = true
        AND OLD.opportunity_name ISNULL
        AND NEW.opportunity_closedate >= {% date_start date_range %}

      UNION ALL

      SELECT
        'Lost' AS category,
        '8' AS order,
        NEW.*
      FROM NEW
      INNER JOIN OLD ON OLD.opportunity_id=NEW.opportunity_id
      INNER JOIN dim_opportunitystage s ON new.opportunity_stage_id=s.id
      INNER JOIN dim_opportunitystage os ON old.opportunity_stage_id=os.id
      WHERE {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        AND s.IsWon=false
        AND s.IsClosed=true
        AND os.IsClosed=false
        AND os.id != '33' --duplicates

      UNION ALL

       SELECT
        'Ending' as category,
        '9' as order,
        NEW.*
      FROM NEW
      INNER JOIN dim_opportunitystage s ON NEW.opportunity_stage_id = s.id
--       AND opportunity_closedate >= {% date_start close_date %}
--      AND opportunity_closedate < {% date_end close_date %}
      WHERE {% condition close_date %} NEW.opportunity_closedate {% endcondition %}
        AND s.isclosed=FALSE
        ANd s.mapped_stage != '0-Pending Acceptance'
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
    link: {
      label: "Explore from here"
      url: "https://gitlab.looker.com/explore/sales/pipeline_change?f[pipeline_change.category]={{ pipeline_change.category }}&f[pipeline_change.close_date]={{ _filters['pipeline_change.close_date'] | url_encode }}&f[pipeline_change.date_range]={{ _filters['pipeline_change.date_range'] | url_encode }}&f[pipeline_change.metric_type]={{ _filters['pipeline_change.metric_type'] | url_encode }}&fields=pipeline_change.opportunity_name,pipeline_change.opportunity_type,dim_opportunitystage.mapped_stage,pipeline_change.total_iacv"
    }
  }

  dimension: order {
    type: number
    sql: ${TABLE}.order ;;
  }

  dimension: opportunity_id {
    hidden: yes
    type: string
    sql: ${TABLE}.opportunity_id ;;
  }

  dimension_group: snapshot {
    type: time
    timeframes: [raw, date]
    sql: ${TABLE}.snapshot_date ;;
  }

  dimension: account_id {
    hidden: yes
    type: number
    sql: ${TABLE}.account_id ;;
  }

  dimension: opportunity_stage_id {
    hidden: yes
    type: number
    sql: ${TABLE}.opportunity_stage_id ;;
  }

  dimension: lead_source_id {
    hidden: yes
    type: number
    sql: ${TABLE}.lead_source_id ;;
  }

  dimension: opportunity_type {
    label: "Type"
    type: string
    sql: ${TABLE}.opportunity_type ;;
  }

  dimension: opportunity_sales_segmentation {
    label: "Sales Segmentation"
    type: string
    sql: ${TABLE}.opportunity_sales_segmentation ;;
  }

  dimension: ownerid {
    hidden: yes
    type: string
    sql: ${TABLE}.ownerid ;;
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
    label: "Close"
    type: time
    timeframes: [raw, date]
    sql: ${TABLE}.opportunity_closedate ;;
  }

  dimension: opportunity_name {
    label: "Name"
    type: string
    sql: ${TABLE}.opportunity_name ;;

    link: {
      label: "Salesforce Opportunity"
      url: "https://na34.salesforce.com/{{ pipeline_change.opportunity_id._value }}"
      icon_url: "https://c1.sfdcstatic.com/etc/designs/sfdc-www/en_us/favicon.ico"
    }
  }

  dimension: iacv {
    label: "IACV"
    hidden: yes
    type: number
    sql: ${TABLE}.iacv ;;
  }

  dimension: renewal_acv {
    label: "Renewal ACV"
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

  measure: count {
    type: count
  }

  set: detail {
    fields: [
    ]
  }
}
