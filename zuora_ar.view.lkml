view: zuora_ar {
  derived_table: {
    sql:
        SELECT zuora_account.entity__c AS entity,
               COALESCE(zuora_contact_bill.workemail,zuora_contact_sold.workemail) AS email,
               zuora_account.name,
               zuora_account.accountnumber,
               zuora_account.currency,
               CASE
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) < 30 THEN '1: <30'
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 30 AND (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) <= 60 THEN '2: 30-60'
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 61 AND (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) <= 90 THEN '3: 61-90'
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 91 THEN '4: >90'
                 ELSE 'Unknown'
               END AS day_range,
               COALESCE(zuora_invoice.balance,0) AS balance,
               zuora_invoice.invoicenumber AS invoice,
               zuora_invoice.duedate as duedate
        FROM zuora.invoice AS zuora_invoice
        INNER JOIN zuora.account AS zuora_account
          ON zuora_invoice.accountid = zuora_account.id
        LEFT JOIN zuora.contact AS zuora_contact_bill
          ON zuora_contact_bill.id = zuora_account.billtocontact
        LEFT JOIN zuora.contact AS zuora_contact_sold
          ON zuora_contact_sold.id = zuora_account.soldtocontactid
        WHERE (zuora_invoice.status = 'Posted')
        AND   zuora_invoice.balance > 0
        ;;
  }
  #
  dimension: 90_days_open_invoices {
    hidden: yes
    type:  string
    sql: ${beyond_90days_open_invoices.list_of_open_invoices} ;;

  }

  dimension: send_email {
    hidden: yes
    sql: ${acct_num} ;;
    html: <a href="https://mail.google.com/mail/?view=cm&fs=1&to={{ email._value }}&cc=apiaseczna@gitlab.com&subject=Invoice-90 Days Past Due?&body=Hi, %0D%0DThe invoice referenced below is 90 past due. In order to keep your GitLab account open.....%0D%0D{{90_days_open_invoices._value}}%0D%0DThanks!%0DGitLab Accounting" target="_blank">
          <img src="https://upload.wikimedia.org/wikipedia/commons/4/4e/Gmail_Icon.png" width="16" height="16"/>
          <a> Click icon to email {{ email._value }}
          ;;
  }
  #
  dimension: entity {
    description: "GitLab Entity"
    type: string
    sql: ${TABLE}.entity ;;
  }
  #
  dimension: email {
    description: "Customer Email"
    type: string
    sql: ${TABLE}.email ;;
  }
  #
  dimension: day_range {
    description: "Account Number of Zuora Customer"
    type: string
    sql: ${TABLE}.day_range ;;
  }
  #
  dimension: invoice {
    description: "Invoice of Customer"
    type: string
    sql: ${TABLE}.invoice ;;
  }
  #
  dimension: customer {
    description: "Customer"
    type: string
    #drill_fields: [drill_1*]
    sql: ${TABLE}.name ;;
  }
  #
  dimension: currency {
    description: "Currency of Customer"
    type: string
    sql: ${TABLE}.currency ;;
  }
  #
  dimension: acct_num {
    description: "Acct # of Customer"
    type: string
    sql: ${TABLE}.accountnumber ;;
  }
  #
  dimension: duedate {
    description: "Due Date of Invoice"
    type: string
    sql: ${TABLE}.duedate ;;
  }
  #
  measure: balance {
    description: "Balance due from Customer"
    type: sum
    sql: ${TABLE}.balance ;;
    drill_fields: [entity,customer,acct_num,90_days_open_invoices,send_email,balance]
  }
  #
  measure: invoice_cnt {
    description: "Count from Customer"
    type: count_distinct
    drill_fields: [entity,customer,acct_num,duedate,balance]
    sql: ${TABLE}.invoice ;;
  }

  measure: count {
    type: count
  }

#   set: drill_1 {
#     fields: [send_email]
#   }

}
