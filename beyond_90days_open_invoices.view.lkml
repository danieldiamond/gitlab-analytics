view: beyond_90days_open_invoices {
  derived_table: {
    sql: with oi as
        (Select zuora_account.name,
             CASE
                      WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) < 30 THEN '1: <30'
                      WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 30 AND (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) <= 60 THEN '2: 30-60'
                      WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 61 AND (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) <= 90 THEN '3: 61-90'
                      WHEN (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 91 THEN '4: >90'
                      ELSE 'Unknown'
                    END AS day_range,
                    string_agg(zuora_invoice.invoicenumber, ', ') over (partition by zuora_account.name) as openinvoices
        from zuora.invoice as zuora_invoice
        inner join zuora.account as zuora_account
          on zuora_invoice.accountid = zuora_account.id
        where (EXTRACT(DAY FROM zuora_invoice.duedate - CURRENT_DATE)*-1) >= 91
        --group by  zuora_account.name,day_range,invoice
      )
      Select oi.name,day_range ,max(oi.openinvoices) as list_of_open_invoices from oi
      group by oi.name, day_range
       ;;
  }

  dimension: name {
    primary_key: yes
    hidden: yes
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: day_range {
    hidden: yes
    type: string
    sql: ${TABLE}.day_range ;;
  }

  dimension: list_of_open_invoices {
    type: string
    sql: ${TABLE}.list_of_open_invoices ;;
  }

#   set: detail {
#     fields: [name, day_range, list_of_open_invoices]
#   }
}
