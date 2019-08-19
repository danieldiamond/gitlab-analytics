{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}


WITH good_events AS (

    SELECT *
    FROM {{ ref('snowplow_unnested_events') }}
    WHERE event_id not in (
        'd1b9015b-f738-4ae7-a4da-a46523a98f15',
        '8de7b076-120b-42b7-922a-d07faded8c8c',
        '1f820848-2b49-4c01-a721-c9d2a2be77a2',
        '246b20a5-b780-4609-b717-b6f3be18c638',  --https://gitlab.com/gitlab-data/analytics/issues/2211
        'b449dfff-8838-452d-9809-82e25aaac737',
        '5cd32226-d848-49b4-8da2-b0a6a60591e3',
        'b9501cea-a9ac-4d3b-9269-313f574aca5a',
        '8de7b076-120b-42b7-922a-d07faded8c8c',
        'c11c4322-e816-4541-99fb-e9bc4df13b7d',
        '94c324fd-a488-4344-85ba-1125c48db62c',
        'b7020f8c-4c9b-4e00-9fe1-49efdb08ac28',
        '24260041-8135-4280-8603-8b157ee6b643',
        '7d28099c-a976-479c-a3c7-4aeac95ca323',
        '9050a947-9f0e-4616-b653-1343f504fda3',
        '23de1087-364c-485a-aed8-c977a11259b7',
        '98775523-afd9-440e-a498-98292b40e074',
        '09b24fea-f1a9-4f42-aa8e-742837eb85fd',
        'abb83dc0-9b84-4a8d-8ef7-2986ae8d5225',
        'b0b1f5f5-f28f-4318-aba9-fe239d627bdc',
        '6e4000b8-1577-4a92-a376-bd0e2156ea9d',
        '29bbb157-c0f5-4f40-b9c3-13f0cc3e7e13',
        '8e3d8901-6618-4911-aaa8-e601a03cda2d',
        'd1b9015b-f738-4ae7-a4da-a46523a98f15',
        'b7f8ec9d-ad2a-4c55-8c9b-a0f50f02ab10',
        '76f30955-f6ae-47a8-808c-ffa0146c256e',
        'eaff3451-671d-43d7-b3a0-46cf96d72829',
        '84811825-4608-4b60-8c65-90b5397fd864',
        'bd411415-64ae-4434-a0a3-a6e3d72a559c',
        '5b6de40f-48ce-48e4-bb7d-18265c6a5e7c',
        '07d6c1a4-bf8e-4c96-9ed5-c61b9686ab29',
        '6ccb0be7-7443-4ac6-a517-16a508475093',
        '1f820848-2b49-4c01-a721-c9d2a2be77a2',
        '246b20a5-b780-4609-b717-b6f3be18c638',
        '377ce680-0f4c-431d-ab42-5bfe57e3366f',
        '41de7acb-6e54-4433-893e-cd05d9fe6269',
        '2fe303c2-d575-414f-b0a4-2107171144f4',
        'e023e017-33ba-4446-8da5-c9937d5dbb47',
        '31808668-1a73-42d6-b16b-12e129c28d12',
        '37ac7cea-944a-45b7-9340-99d6cbe03552' --https://gitlab.com/gitlab-data/analytics/issues/2211
        )

),

bad_events AS (

    SELECT *
    FROM {{ ref('snowplow_unnested_errors') }}

),

good_count AS (

    SELECT
        date_trunc('day',derived_tstamp ::timestamp)::date  AS event_day,
        count(*)                                            AS good_event_count
    FROM good_events
    GROUP BY 1

),

bad_count AS (

    SELECT
        date_trunc('day',failure_timestamp ::timestamp)::date   AS event_day,
        count(*)                                                AS bad_event_count
    FROM bad_events
    GROUP BY 1

)

SELECT
    good_count.event_day,
    good_count.good_event_count,
    bad_count.bad_event_count
FROM good_count
LEFT JOIN bad_count on good_count.event_day = bad_count.event_day
ORDER BY event_day
