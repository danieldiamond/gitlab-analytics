view: zuora_current_arr {
  derived_table: {
    sql:
        WITH arr AS
        (
          SELECT s_1.id,
                 SUM(c_1.mrr*12::NUMERIC) AS current_arr
          FROM zuora.subscription s_1
            JOIN zuora.account a_1 ON s_1.accountid = a_1.id::TEXT
            JOIN zuora.rateplan r_1 ON r_1.subscriptionid::TEXT = s_1.id
            JOIN zuora.rateplancharge c_1 ON c_1.rateplanid::TEXT = r_1.id::TEXT
          WHERE (s_1.status <> ALL (ARRAY['Draft'::TEXT,'Expired'::TEXT]))
          AND   c_1.effectivestartdate <= CURRENT_DATE
          AND   (c_1.effectiveenddate > CURRENT_DATE OR c_1.effectiveenddate IS NULL)
          AND   (s_1.excludefromanalysis__c = FALSE OR s_1.excludefromanalysis__c IS NULL)
          GROUP BY s_1.id
        )

        SELECT SUM(CASE WHEN current_arr > 0 THEN 1 ELSE 0 END) AS over_0,
               SUM(CASE WHEN current_arr > 5000 THEN 1 ELSE 0 END) AS over_5k,
               SUM(CASE WHEN current_arr > 50000 THEN 1 ELSE 0 END) AS over_50k,
               SUM(CASE WHEN current_arr > 100000 THEN 1 ELSE 0 END) AS over_100k,
               SUM(current_arr) AS current_arr
        FROM arr
        ;;
  }
  #
  measure: over_0 {
    description: "Over 0 Customers"
    type: number
    value_format: "#,##0"
    sql: ${TABLE}.over_0 ;;
  }
  #
  measure: over_5k {
    description: "Over 5K Customers"
    type: number
    value_format: "#,##0"
    sql: ${TABLE}.over_5k ;;
  }
  #
  measure: over_50k {
    description: "Over 50K Customers"
    type: number
    value_format: "#,##0"
    sql: ${TABLE}.over_50k ;;
  }
  #
  measure: over_100k {
    description: "Over 100K Customers"
    type: number
    value_format: "#,##0"
    sql: ${TABLE}.over_100k ;;
  }
  #
  measure: current_arr {
    description: "Current ARR"
    type: number
    value_format: "$#,##0"
    sql: ${TABLE}.current_arr ;;
  }

}
