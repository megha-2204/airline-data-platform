SELECT
    p.FLIGHT_ID,
    p.FLIGHT_NUMBER,

    -- Flight capacity and current booking state
    p.TOTAL_SEATS,
    p.SEATS_BOOKED,
    p.OCCUPANCY_PERCENT,

    -- Real-time Spark demand signals
    rp.SEARCH_COUNT,
    rp.SEARCH_SIGNAL,

    rp.BOOKING_COUNT,
    rp.SEATS_BOOKED AS REALTIME_SEATS_BOOKED,
    rp.OCCUPANCY,

    rp.BOOKING_REVENUE,

    -- Demand and pricing
    rp.DEMAND_SCORE,
    rp.DAYS_TO_DEPARTURE,
    rp.PRICE_MULTIPLIER,
    rp.SIMULATED_PRICE,

    -- Batch financial context
    p.TOTAL_REVENUE,

    ROUND(
            p.TOTAL_REVENUE / NULLIF(p.SEATS_BOOKED, 0),
            2
    ) AS AVERAGE_BOOKING_VALUE,

    -- Revenue if the current demand-based multiplier
    -- were applied to the existing revenue
    ROUND(
            p.TOTAL_REVENUE * rp.PRICE_MULTIPLIER,
            2
    ) AS SIMULATED_ADJUSTED_REVENUE,

    rp.PROCESSED_AT

FROM {{ ref('int_flight_performance') }} p

JOIN {{ ref('stg_flight_pricing') }} rp
ON p.FLIGHT_ID = rp.FLIGHT_ID