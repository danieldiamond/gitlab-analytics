view: user {
  # This is a raw view on sfdc.user.
  sql_table_name: sfdc."user" ;;
  label: "SFDC User"

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: alias {
    type: string
    sql: ${TABLE}.alias ;;
  }

  dimension: communitynickname {
    label: "Nickname"
    type: string
    sql: ${TABLE}.communitynickname ;;
  }

  dimension: companyname {
    label: "Company Name"
    type: string
    sql: ${TABLE}.companyname ;;
  }

  dimension: department {
    type: string
    sql: ${TABLE}.department ;;
  }

  dimension: managerid {
    label: "Manager ID"
    type: string
    sql: ${TABLE}.managerid ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: profileid {
    label: "Profile ID"
    type: string
    sql: ${TABLE}.profileid ;;
  }

  dimension: team__c {
    label: "Team"
    type: string
    sql: ${TABLE}.team__c ;;
  }

  dimension: timezonesidkey {
    label: "Time Zone"
    type: string
    sql: ${TABLE}.timezonesidkey ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: username {
    type: string
    sql: ${TABLE}.username ;;
  }

  dimension: userroleid {
    label: "User Role ID"
    type: string
    sql: ${TABLE}.userroleid ;;
  }

  dimension: usertype {
    label: "User Type"
    type: string
    sql: ${TABLE}.usertype ;;
  }
}
