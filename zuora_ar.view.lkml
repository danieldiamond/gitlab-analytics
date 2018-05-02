view: zuora_ar {
  derived_table: {
    sql:
        SELECT CASE
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) < 30 THEN '<30'
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 30 AND (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) <= 60 THEN '30-60'
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 61 AND (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) <= 90 THEN '61-90'
                 WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 91 THEN '>90'
                 ELSE 'Unknown'
               END AS "zuora_invoice.day_range",
               COALESCE(zuora_invoice.balance,0) AS "zuora_invoice.balance",
               zuora_invoice.invoicenumber AS "zuora_invoice.invoice"
        FROM zuora.invoice AS zuora_invoice
        WHERE (zuora_invoice.status = 'Posted')
        AND   zuora_invoice.balance > 0
        ;;
  }
}
