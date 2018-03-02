view: dim_account {
  sql_table_name: analytics.dim_account ;;

  dimension: id {
    primary_key: yes
    hidden: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: industry {
    type: string
    sql: ${TABLE}.industry ;;
  }

  dimension: is_lau {
    type: yesno
    sql: ${TABLE}.is_lau ;;
  }

  dimension: name {
    description: "The Name of the Salesforce Account."
    type: string
    sql: ${TABLE}.name ;;

    link: {
      label: "Salesforce Account"
      url: "https://na34.salesforce.com/{{ dim_account.sfdc_account_id }}"
      icon_url: "https://c1.sfdcstatic.com/etc/designs/sfdc-www/en_us/favicon.ico"
    }

  }

  dimension: sales_segmentation {
    type: string
    description: "The Sales Segmentation for this account."
    sql: ${TABLE}.sales_segmentation ;;
  }

  dimension: sfdc_account_id {
    description: "This is the Salesforce account Id."
    hidden: yes
    type: string
    sql: ${TABLE}.sfdc_account_id ;;
  }

  dimension: type {
    type: string
    label: "Account Type"
    sql: ${TABLE}.type ;;
  }

  dimension: ultimate_parent_name {
    label: "Ultimate Parent Name"
    type: string
    sql: ${TABLE}.ultimate_parent_name ;;
  }

  dimension: ultimate_parent_sales_segmentation {
    label: "Ultimate Parent Sales Segmentation"
    type: string
    sql: ${TABLE}.ultimate_parent_sales_segmentation ;;
  }

  measure: count {
    type: count
    drill_fields: [id, ultimate_parent_name, name]
  }
}
