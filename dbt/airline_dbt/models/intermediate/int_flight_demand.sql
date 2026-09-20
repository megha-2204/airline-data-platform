WITH search_data AS (
    SELECT
        FLIGHT_ID,
        COUNT(*) AS TOTAL_SEARCHES,

        -- Searches in the last 1 hour
        COUNT_IF(
                SEARCHED_AT >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        ) AS SEARCHES_LAST_1_HOUR

    FROM {{ ref('stg_searches') }}
    GROUP BY FLIGHT_ID
),

     booking_data AS (
         SELECT
             FLIGHT_ID,
             COUNT(*) AS TOTAL_BOOKINGS,
             SUM(SEAT_COUNT) AS TOTAL_SEATS_BOOKED,

             -- Bookings in the last 1 hour
             COUNT_IF(
                     BOOKING_TIME >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
             ) AS BOOKINGS_LAST_1_HOUR

         FROM {{ ref('stg_bookings') }}
         WHERE STATUS = 'CONFIRMED'
         GROUP BY FLIGHT_ID
     )

SELECT
    f.FLIGHT_ID,
    f.FLIGHT_NUMBER,

    COALESCE(s.TOTAL_SEARCHES, 0) AS TOTAL_SEARCHES,
    COALESCE(s.SEARCHES_LAST_1_HOUR, 0) AS SEARCHES_LAST_1_HOUR,

    COALESCE(b.TOTAL_BOOKINGS, 0) AS TOTAL_BOOKINGS,
    COALESCE(b.TOTAL_SEATS_BOOKED, 0) AS TOTAL_SEATS_BOOKED,
    COALESCE(b.BOOKINGS_LAST_1_HOUR, 0) AS BOOKINGS_LAST_1_HOUR,

    -- Velocity = activity per hour
    COALESCE(s.SEARCHES_LAST_1_HOUR, 0) AS SEARCH_VELOCITY,
    COALESCE(b.BOOKINGS_LAST_1_HOUR, 0) AS BOOKING_VELOCITY,

    -- Simple demand score
    COALESCE(s.SEARCHES_LAST_1_HOUR, 0)
        + (COALESCE(b.BOOKINGS_LAST_1_HOUR, 0) * 5) AS DEMAND_SCORE

FROM {{ ref('stg_flights') }} f

LEFT JOIN search_data s
ON f.FLIGHT_ID = s.FLIGHT_ID

    LEFT JOIN booking_data b
    ON f.FLIGHT_ID = b.FLIGHT_ID