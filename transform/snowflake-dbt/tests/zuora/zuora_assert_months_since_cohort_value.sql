-- Ensures that the months since the start of the cohort can never be less than 0.
-- Filters are on accounts that finance has already been alerted need fixing
-- We only use the hash when the slugs have identifying customer info

SELECT md5(subscription_name_slugify) AS new_slug
FROM {{ ref('zuora_mrr_totals') }}
WHERE MONTHS_SINCE_ZUORA_SUBSCRIPTION_COHORT_START < 0
  AND md5(subscription_name_slugify) NOT IN (
    'aff8570188adbd05b56e638dc8caa0ff',
    '623effd7a33fcdb482a9a391819f1a33',
    'b2f4148b08c271b9de21c448d4844d50',
    'b9c472eea8eb7c9643ba703ae94cdf29',
    'a5b1c87c8e8b1575e6f1301e246ef57a',
    'f52245e16b1008b2870e18b2d850ea9b',
    'bf6e4bac11106dcbf456290bee0b2bd1',
    '68f27986777f7cae423e365d88db5bc4',
    '8138ebbcf4eb41efe668eb1964c0a498',
    '079d10a8e4b4afd8aa5ef17ef27c4b21',
    '61253d540897e73bb818db73fbf93dd7',
    'aac85a45ef2a2f8bc0d2a53d612feee1'
    )
