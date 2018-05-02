view: zuora_ar {
  derived_table: {
    sql:
        SELECT CASE
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) < 30 THEN '<30'
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 30 AND (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) <= 60 THEN '30-60'
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 61 AND (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) <= 90 THEN '61-90'
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 91 THEN '>90'
                 ELSE 'Unknown'
               END AS "day_range",
               COALESCE(zuora_invoice.balance,0) AS "balance",
               zuora_invoice.invoicenumber AS "invoice"
        FROM zuora.invoice AS zuora_invoice
        WHERE (zuora_invoice.status = 'Posted')
        AND   zuora_invoice.balance > 0
        ;;
  }
  #
  dimension: account_number {
    description: "Account Number of Zuora Customer"
    type: string
    sql: ${TABLE}.day_range ;;
  }
  #
  dimension: invoice {
    description: "Invoice of Zuora Customer"
    type: string
    sql: ${TABLE}.invoice ;;
  }
  #
  measure: balance {
    description: "Balance due from Customer"
    type: sum
    sql: ${TABLE}.balance ;;
  }
  #
  measure: invoice_cnt {
    description: "Balance due from Customer"
    type: count_distinct
    sql: ${TABLE}.invoice ;;
  }

}
