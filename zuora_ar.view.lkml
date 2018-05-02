view: zuora_ar {
  derived_table: {
    sql:
        SELECT zuora_account.name,
               zuora_account.accountnumber,
               zuora_account.currency,
               CASE
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) < 30 THEN '<30'
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 30 AND (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) <= 60 THEN '30-60'
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 61 AND (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) <= 90 THEN '61-90'
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 91 THEN '>90'
                 ELSE 'Unknown'
               END AS day_range,
               COALESCE(zuora_invoice.balance,0) AS balance,
               zuora_invoice.invoicenumber AS invoice,
               zuora_invoice.duedate
        FROM zuora.invoice AS zuora_invoice
        INNER JOIN zuora.account AS zuora_account
          ON zuora_invoice.accountid = zuora_account.id
        WHERE (zuora_invoice.status = 'Posted')
        AND   zuora_invoice.balance > 0
        ;;
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
  }
  #
  measure: invoice_cnt {
    description: "Count from Customer"
    type: count_distinct
    sql: ${TABLE}.invoice ;;
  }

}
