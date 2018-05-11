view: userrole {
  label: "SFDC User Role"
  sql_table_name: sfdc.userrole ;;

  dimension: id {
    primary_key: yes
    type: string
    hidden: yes
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    label: "Role Name"
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: opportunityaccessforaccountowner {
    label: "Opportunity Permissions"
    type: string
    sql: ${TABLE}.opportunityaccessforaccountowner ;;
  }

  dimension: parentroleid {
    hidden: yes
    type: string
    sql: ${TABLE}.parentroleid ;;
  }
}
