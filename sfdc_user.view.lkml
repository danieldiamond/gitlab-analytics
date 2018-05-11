view: user {
  sql_table_name: sfdc."user" ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: aboutme {
    type: string
    sql: ${TABLE}.aboutme ;;
  }

  dimension: accountid {
    type: string
    sql: ${TABLE}.accountid ;;
  }

  dimension: alias {
    type: string
    sql: ${TABLE}.alias ;;
  }

  dimension: badgetext {
    type: string
    sql: ${TABLE}.badgetext ;;
  }

  dimension: callcenterid {
    type: string
    sql: ${TABLE}.callcenterid ;;
  }

  dimension: can_change_bdr_on_lead__c {
    type: yesno
    sql: ${TABLE}.can_change_bdr_on_lead__c ;;
  }

  dimension: can_change_lead_or_contact_owner__c {
    type: yesno
    sql: ${TABLE}.can_change_lead_or_contact_owner__c ;;
  }

  dimension: can_change_sdr_on_account__c {
    type: yesno
    sql: ${TABLE}.can_change_sdr_on_account__c ;;
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }

  dimension: communitynickname {
    type: string
    sql: ${TABLE}.communitynickname ;;
  }

  dimension: companyname {
    type: string
    sql: ${TABLE}.companyname ;;
  }

  dimension: contactid {
    type: string
    sql: ${TABLE}.contactid ;;
  }

  dimension: country {
    type: string
    map_layer_name: countries
    sql: ${TABLE}.country ;;
  }

  dimension: countrycode {
    type: string
    sql: ${TABLE}.countrycode ;;
  }

  dimension: createdbyid {
    type: string
    sql: ${TABLE}.createdbyid ;;
  }

  dimension_group: createddate {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.createddate ;;
  }

  dimension: defaultgroupnotificationfrequency {
    type: string
    sql: ${TABLE}.defaultgroupnotificationfrequency ;;
  }

  dimension: delegatedapproverid {
    type: string
    sql: ${TABLE}.delegatedapproverid ;;
  }

  dimension: department {
    type: string
    sql: ${TABLE}.department ;;
  }

  dimension: digestfrequency {
    type: string
    sql: ${TABLE}.digestfrequency ;;
  }

  dimension: division {
    type: string
    sql: ${TABLE}.division ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: emailencodingkey {
    type: string
    sql: ${TABLE}.emailencodingkey ;;
  }

  dimension: emailpreferencesautobcc {
    type: yesno
    sql: ${TABLE}.emailpreferencesautobcc ;;
  }

  dimension: emailpreferencesautobccstayintouch {
    type: yesno
    sql: ${TABLE}.emailpreferencesautobccstayintouch ;;
  }

  dimension: emailpreferencesstayintouchreminder {
    type: yesno
    sql: ${TABLE}.emailpreferencesstayintouchreminder ;;
  }

  dimension: employeenumber {
    type: string
    sql: ${TABLE}.employeenumber ;;
  }

  dimension: extension {
    type: string
    sql: ${TABLE}.extension ;;
  }

  dimension: fax {
    type: string
    sql: ${TABLE}.fax ;;
  }

  dimension: federationidentifier {
    type: string
    sql: ${TABLE}.federationidentifier ;;
  }

  dimension: finance__c {
    type: string
    sql: ${TABLE}.finance__c ;;
  }

  dimension: firstname {
    type: string
    sql: ${TABLE}.firstname ;;
  }

  dimension: forecastenabled {
    type: yesno
    sql: ${TABLE}.forecastenabled ;;
  }

  dimension: geocodeaccuracy {
    type: string
    sql: ${TABLE}.geocodeaccuracy ;;
  }

  dimension: isactive {
    type: yesno
    sql: ${TABLE}.isactive ;;
  }

  dimension: isextindicatorvisible {
    type: yesno
    sql: ${TABLE}.isextindicatorvisible ;;
  }

  dimension: isportalenabled {
    type: yesno
    sql: ${TABLE}.isportalenabled ;;
  }

  dimension: isprmsuperuser {
    type: yesno
    sql: ${TABLE}.isprmsuperuser ;;
  }

  dimension: isprofilephotoactive {
    type: yesno
    sql: ${TABLE}.isprofilephotoactive ;;
  }

  dimension: languagelocalekey {
    type: string
    sql: ${TABLE}.languagelocalekey ;;
  }

  dimension_group: lastlogindate {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.lastlogindate ;;
  }

  dimension: lastmodifiedbyid {
    type: string
    sql: ${TABLE}.lastmodifiedbyid ;;
  }

  dimension_group: lastmodifieddate {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.lastmodifieddate ;;
  }

  dimension: lastname {
    type: string
    sql: ${TABLE}.lastname ;;
  }

  dimension_group: lastpasswordchangedate {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.lastpasswordchangedate ;;
  }

  dimension_group: lastreferenceddate {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.lastreferenceddate ;;
  }

  dimension_group: lastvieweddate {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.lastvieweddate ;;
  }

  dimension: latitude {
    type: string
    sql: ${TABLE}.latitude ;;
  }

  dimension: localesidkey {
    type: string
    sql: ${TABLE}.localesidkey ;;
  }

  dimension: longitude {
    type: string
    sql: ${TABLE}.longitude ;;
  }

  dimension: managerid {
    type: string
    sql: ${TABLE}.managerid ;;
  }

  dimension: middlename {
    type: string
    sql: ${TABLE}.middlename ;;
  }

  dimension: mkto_si__iscachinganonwebactivitylist__c {
    type: yesno
    sql: ${TABLE}.mkto_si__iscachinganonwebactivitylist__c ;;
  }

  dimension: mkto_si__iscachingbestbets__c {
    type: yesno
    sql: ${TABLE}.mkto_si__iscachingbestbets__c ;;
  }

  dimension: mkto_si__iscachingemailactivitylist__c {
    type: yesno
    sql: ${TABLE}.mkto_si__iscachingemailactivitylist__c ;;
  }

  dimension: mkto_si__iscachinggroupedwebactivitylist__c {
    type: yesno
    sql: ${TABLE}.mkto_si__iscachinggroupedwebactivitylist__c ;;
  }

  dimension: mkto_si__iscachinginterestingmomentslist__c {
    type: yesno
    sql: ${TABLE}.mkto_si__iscachinginterestingmomentslist__c ;;
  }

  dimension: mkto_si__iscachingscoringlist__c {
    type: yesno
    sql: ${TABLE}.mkto_si__iscachingscoringlist__c ;;
  }

  dimension: mkto_si__iscachingstreamlist__c {
    type: yesno
    sql: ${TABLE}.mkto_si__iscachingstreamlist__c ;;
  }

  dimension: mkto_si__iscachingwatchlist__c {
    type: yesno
    sql: ${TABLE}.mkto_si__iscachingwatchlist__c ;;
  }

  dimension: mkto_si__iscachingwebactivitylist__c {
    type: yesno
    sql: ${TABLE}.mkto_si__iscachingwebactivitylist__c ;;
  }

  dimension: mkto_si__sales_insight_counter__c {
    type: number
    sql: ${TABLE}.mkto_si__sales_insight_counter__c ;;
  }

  dimension: mobilephone {
    type: string
    sql: ${TABLE}.mobilephone ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension_group: offlinepdatrialexpirationdate {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.offlinepdatrialexpirationdate ;;
  }

  dimension_group: offlinetrialexpirationdate {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.offlinetrialexpirationdate ;;
  }

  dimension: phone {
    type: string
    sql: ${TABLE}.phone ;;
  }

  dimension: portalrole {
    type: string
    sql: ${TABLE}.portalrole ;;
  }

  dimension: postalcode {
    type: string
    sql: ${TABLE}.postalcode ;;
  }

  dimension: profileid {
    type: string
    sql: ${TABLE}.profileid ;;
  }

  dimension: receivesadmininfoemails {
    type: yesno
    sql: ${TABLE}.receivesadmininfoemails ;;
  }

  dimension: receivesinfoemails {
    type: yesno
    sql: ${TABLE}.receivesinfoemails ;;
  }

  dimension: senderemail {
    type: string
    sql: ${TABLE}.senderemail ;;
  }

  dimension: sendername {
    type: string
    sql: ${TABLE}.sendername ;;
  }

  dimension: signature {
    type: string
    sql: ${TABLE}.signature ;;
  }

  dimension_group: start_date__c {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.start_date__c ;;
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: statecode {
    type: string
    sql: ${TABLE}.statecode ;;
  }

  dimension: stayintouchnote {
    type: string
    sql: ${TABLE}.stayintouchnote ;;
  }

  dimension: stayintouchsignature {
    type: string
    sql: ${TABLE}.stayintouchsignature ;;
  }

  dimension: stayintouchsubject {
    type: string
    sql: ${TABLE}.stayintouchsubject ;;
  }

  dimension: street {
    type: string
    sql: ${TABLE}.street ;;
  }

  dimension: suffix {
    type: string
    sql: ${TABLE}.suffix ;;
  }

  dimension_group: systemmodstamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.systemmodstamp ;;
  }

  dimension: team__c {
    type: string
    sql: ${TABLE}.team__c ;;
  }

  dimension: timezonesidkey {
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
    type: string
    sql: ${TABLE}.userroleid ;;
  }

  dimension: usertype {
    type: string
    sql: ${TABLE}.usertype ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      id,
      communitynickname,
      companyname,
      firstname,
      lastname,
      middlename,
      name,
      sendername,
      username
    ]
  }
}
