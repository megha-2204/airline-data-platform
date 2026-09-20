SELECT
    p.FLIGHT_ID,
    p.FLIGHT_NUMBER,
    p.TOTAL_SEATS,
    p.SEATS_BOOKED,
    p.TOTAL_REVENUE,
    p.OCCUPANCY_PERCENT,

    d.TOTAL_SEARCHES,
    d.SEARCHES_LAST_1_HOUR,
    d.SEARCH_VELOCITY,

    d.TOTAL_BOOKINGS,
    d.BOOKINGS_LAST_1_HOUR,
    d.BOOKING_VELOCITY,

    d.DEMAND_SCORE,

    DATEDIFF(
            'day',
            CURRENT_TIMESTAMP(),
            p.DEPARTURE_TIME
    ) AS DAYS_TO_DEPARTURE,

    CASE
        WHEN DATEDIFF('day', CURRENT_TIMESTAMP(), p.DEPARTURE_TIME) <= 2
            AND (
                 p.OCCUPANCY_PERCENT >= 80
                     OR d.DEMAND_SCORE >= 100
                 )
            THEN 1.50

        WHEN DATEDIFF('day', CURRENT_TIMESTAMP(), p.DEPARTURE_TIME) <= 7
            AND (
                 p.OCCUPANCY_PERCENT >= 60
                     OR d.DEMAND_SCORE >= 75
                 )
            THEN 1.30

        WHEN p.OCCUPANCY_PERCENT >= 60
            OR d.DEMAND_SCORE >= 75
            THEN 1.20

        WHEN p.OCCUPANCY_PERCENT >= 40
            OR d.DEMAND_SCORE >= 50
            THEN 1.10

        ELSE 1.00
        END AS PRICE_MULTIPLIER

FROM {{ ref('int_flight_performance') }} p

JOIN {{ ref('int_flight_demand') }} d
ON p.FLIGHT_ID = d.FLIGHT_ID