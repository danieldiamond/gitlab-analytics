WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'calendar') }}

), renamed AS (

  SELECT
    NULLIF("Event_Title", '')::varchar as event_title,
    NULLIF("Event_Location", '')::varchar as event_location,
    NULLIF("Event_Start", '')::varchar::date as event_start,
    NULLIF(NULLIF("Calculated_Duration"::varchar, '#REF!'), '')::float as calculated_duration,
    NULLIF("Date_Created", '')::varchar::date as date_created,
    NULLIF("Created_By", '')::varchar as created_by
  FROM source

), categorized AS (

    SELECT *,
      CASE WHEN lower(created_by) = 'karen.sijbrandij@gmail.com' THEN 'Personal'
					 WHEN lower(event_title) LIKE '%company call%' THEN 'Company Call'
           WHEN lower(event_title) LIKE '%fgu%' THEN 'Group Conversation'
           WHEN lower(event_title) LIKE '%group conversation%' THEN 'Group Conversation'
           WHEN lower(event_title) LIKE '%Monthly Diversity & Inclusion Initiatives Call%' THEN 'Diversity Initiatives'
           WHEN lower(event_title) LIKE '%e-group%' THEN 'E-Group'
           WHEN lower(event_title) LIKE '%key monthly review%' THEN 'Monthly Key Review'
           WHEN lower(event_title) LIKE '%monthly key review%' THEN 'Monthly Key Review'
           WHEN event_title LIKE '%MTG%' THEN 'In Person Meetings'
           WHEN event_title LIKE '%INTERVIEW%' THEN 'Media Interviews'
           WHEN lower(event_title) LIKE '%media briefing%' THEN 'Media Interviews'
           WHEN lower(event_title) LIKE '%highwire%' THEN 'Media Interviews'
           WHEN event_title LIKE '%CALL%' THEN 'Conference Calls'
           WHEN event_title LIKE '%VIDEOCALL%' THEN 'Video Calls'
           WHEN event_title LIKE '%LIVESTREAM%' THEN 'Livestreams'
           WHEN lower(event_title) LIKE '%interview%' THEN 'Candidate Interviews/Hiring'
           WHEN lower(event_title) LIKE '%ejento%' THEN 'Candidate Interviews/Hiring'
           WHEN lower(event_title) LIKE '%reference call%' THEN 'Candidate Interviews/Hiring'
           WHEN event_title LIKE '%1:1%' THEN 'One on ones'
           WHEN lower(event_title) LIKE '%skip level%' THEN 'Skip Levels'
           WHEN lower(event_title) LIKE '%flight%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%uber%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%ground transportation%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%car service%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%ua%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%travel%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%drive%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%driving%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%walk%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%tsa%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%airport%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%check-in%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%check-out%' THEN 'Travel'
           WHEN lower(event_title) LIKE '%accent reduction%' THEN 'Personal'
           WHEN lower(event_title) LIKE '%pa anet%' THEN 'Personal'
           WHEN lower(event_title) LIKE '%brother%' THEN 'Personal'
           WHEN lower(event_title) LIKE '%sister%' THEN 'Personal'
           WHEN lower(event_title) LIKE '%parents%' THEN 'Personal'
           WHEN lower(event_title) LIKE '%double gdp%' THEN 'Personal'
           WHEN lower(event_title) LIKE '%niece%' THEN 'Personal'
           WHEN lower(event_title) LIKE '%personal%' THEN 'Personal'
           WHEN lower(event_title) LIKE '%Karen/Sid%' THEN 'Personal'
           WHEN lower(event_title) LIKE '%bio%' THEN 'Personal'
           WHEN event_title IN ('Scaling: CEO', 'PM & Engineering Weekly') THEN 'Product Leadership'
           WHEN lower(event_title) LIKE '%product strategy%' THEN 'Product Leadership'
           WHEN event_title LIKE '%ICONIQ%' THEN 'Board related'
           WHEN event_title LIKE '%Board Dinner%' THEN 'Board related'
           WHEN event_title LIKE '%Board of Directors%' THEN 'Board related'
           WHEN event_title LIKE '%Board Meeting%' THEN 'Board related'
					 WHEN lower(event_title) LIKE '%exec time%' THEN 'Executive Time'
					 WHEN lower(event_title) LIKE '%executive time%' THEN 'Executive Time'
      ELSE 'Other'
      END AS event_category

    FROM renamed
    WHERE calculated_duration > 0
    AND lower(event_title) NOT LIKE '%fyi%'
    AND lower(event_title) NOT LIKE '%fya%'

), final as (

    SELECT
      md5(event_title) as masked_event_title,
      event_start,
      CASE WHEN right(((round(calculated_duration*4))*.25)::varchar, 3) = '.75' THEN ((round(calculated_duration*4))*.25+.25) ELSE (round(calculated_duration*4))*.25 END as calculated_duration,
      event_category,
      CASE WHEN event_category IN ('Monthly Key Review', 'Media Interviews', 'Livestreams', 'In Person Meetings', 'Conference Calls') THEN '1. IACV'
           WHEN event_category IN ('Product Leadership') THEN '2. Popular next generation product'
           WHEN event_category IN ('Skip Levels', 'One on ones', 'Group Conversation', 'E-Group', 'Company Call', 'Candidate Interviews/Hiring') THEN '3. Great team'
           WHEN event_category IN ('Travel', 'Personal', 'Other') THEN 'Miscellaneous'
           WHEN event_category IN ('Board related', 'Executive Time') THEN 'Executive Responsibilities'
      ELSE NULL END AS okr_time_allocation
    FROM categorized
		WHERE event_category != 'Personal'

)

SELECT *
FROM final
