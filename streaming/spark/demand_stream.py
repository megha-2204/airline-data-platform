from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    count,
    sum,
    avg,
    least,
    greatest,
    round,
    lit,
    unix_timestamp,
    to_timestamp,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    DoubleType,
    StringType,
)
import os


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("AirlineDemandPricing")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Read search-demand metrics produced by search_demand.py
# ============================================================

search_metrics_df = spark.read.parquet(
    "/tmp/airline_search_metrics"
)


# ============================================================
# Simulation time
# ============================================================

simulation_time = "2026-09-07 10:15:31"


# ============================================================
# Snowflake configuration
# ============================================================

SNOWFLAKE_OPTIONS = {
    "sfURL": "YJULKYN-RZ18732.snowflakecomputing.com",
    "sfUser": "MEGHAB2204NEW",
    "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
    "sfDatabase": "AIRLINE_DB",
    "sfSchema": "RAW",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
}


# ============================================================
# Real flight metadata from PostgreSQL
# ============================================================

flight_metadata = [
    (1, 180, "2026-09-11 15:08:45.794823"),
    (2, 250, "2026-09-13 06:08:45.794823"),
    (3, 250, "2026-09-10 17:08:45.794823"),
    (4, 189, "2026-09-08 02:08:45.794823"),
    (5, 250, "2026-09-08 17:08:45.794823"),
    (6, 220, "2026-09-11 08:08:45.794823"),
    (7, 250, "2026-09-08 18:08:45.794823"),
    (8, 250, "2026-09-12 10:08:45.794823"),
    (9, 180, "2026-09-10 20:08:45.794823"),
    (10, 180, "2026-09-13 11:08:45.794823"),
    (11, 250, "2026-09-09 12:08:45.794823"),
    (12, 220, "2026-09-13 23:08:45.794823"),
    (13, 180, "2026-09-10 04:08:45.794823"),
    (14, 220, "2026-09-13 09:08:45.794823"),
    (15, 180, "2026-09-14 07:08:45.794823"),
    (16, 189, "2026-09-08 09:08:45.794823"),
    (17, 189, "2026-09-08 13:08:45.794823"),
    (18, 250, "2026-09-12 00:08:45.794823"),
    (19, 250, "2026-09-09 22:08:45.794823"),
    (20, 220, "2026-09-11 15:08:45.794823"),
    (21, 250, "2026-09-09 06:08:45.794823"),
    (22, 220, "2026-09-13 13:08:45.794823"),
    (23, 189, "2026-09-13 17:08:45.794823"),
    (24, 189, "2026-09-10 09:08:45.794823"),
    (25, 180, "2026-09-11 20:08:45.794823"),
    (26, 180, "2026-09-09 23:08:45.794823"),
    (27, 180, "2026-09-08 00:08:45.794823"),
    (28, 250, "2026-09-08 15:08:45.794823"),
    (29, 250, "2026-09-11 18:08:45.794823"),
    (30, 250, "2026-09-08 21:08:45.794823"),
]


flight_capacity_df = spark.createDataFrame(
    flight_metadata,
    ["flight_id", "total_seats", "departure_time"]
)


flight_capacity_df = flight_capacity_df.withColumn(
    "departure_time",
    to_timestamp("departure_time")
)


# ============================================================
# Kafka schemas
# ============================================================

booking_schema = StructType([
    StructField("booking_id", LongType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("flight_id", IntegerType(), True),
    StructField("booking_time", StringType(), True),
    StructField("seat_count", IntegerType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("status", StringType(), True),
])


search_schema = StructType([
    StructField("search_id", LongType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("flight_id", IntegerType(), True),
    StructField("searched_at", StringType(), True),
])


# ============================================================
# Read bookings from Kafka
# ============================================================

bookings_raw = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "airline_bookings"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)


bookings = (
    bookings_raw
    .select(
        from_json(
            col("value").cast("string"),
            booking_schema
        ).alias("data")
    )
    .select("data.*")
    .filter(
        col("flight_id").isNotNull()
    )
)


# ============================================================
# Read searches from Kafka
# ============================================================

searches_raw = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "airline_searches"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)


searches = (
    searches_raw
    .select(
        from_json(
            col("value").cast("string"),
            search_schema
        ).alias("data")
    )
    .select("data.*")
    .filter(
        col("flight_id").isNotNull()
    )
)


# ============================================================
# Booking metrics
# ============================================================

booking_metrics = (
    bookings
    .groupBy("flight_id")
    .agg(
        count("*").alias("booking_count"),
        sum("seat_count").alias("seats_booked"),
        sum("total_amount").alias("booking_revenue"),
        avg("total_amount").alias("average_booking_value"),
    )
)


# ============================================================
# Search metrics
# ============================================================

search_metrics = (
    searches
    .groupBy("flight_id")
    .agg(
        count("*").alias("search_count"),
    )
)


# ============================================================
# Demand + pricing calculation
# ============================================================

def calculate_pricing(batch_df, batch_id):

    print()
    print("=" * 70)
    print(f"DEMAND / PRICING BATCH: {batch_id}")
    print("=" * 70)

    # --------------------------------------------------------
    # Combine booking metrics + flight capacity + search metrics
    # --------------------------------------------------------

    booking_df = (
        batch_df
        .select(
            "flight_id",
            "booking_count",
            "seats_booked",
            "booking_revenue",
            "average_booking_value",
        )
        .join(
            flight_capacity_df,
            on="flight_id",
            how="left"
        )
        .join(
            search_metrics_df,
            on="flight_id",
            how="left"
        )
        .fillna({
            "search_count": 0,
            "search_signal": 0.0
        })
    )


    # --------------------------------------------------------
    # Days to departure
    # --------------------------------------------------------

    booking_df = booking_df.withColumn(
        "days_to_departure",
        (
                unix_timestamp(col("departure_time"))
                - unix_timestamp(lit(simulation_time))
        ) / lit(86400.0)
    )


    # --------------------------------------------------------
    # Pricing calculations
    # --------------------------------------------------------

    pricing_df = (
        booking_df

        # Booking signal
        .withColumn(
            "booking_signal",
            least(
                greatest(
                    col("booking_count") / lit(20.0),
                    lit(0.0)
                ),
                lit(1.0)
            )
        )

        # Occupancy
        .withColumn(
            "occupancy",
            col("seats_booked") / col("total_seats")
        )

        # Occupancy signal
        .withColumn(
            "occupancy_signal",
            least(
                greatest(
                    col("occupancy"),
                    lit(0.0)
                ),
                lit(1.0)
            )
        )

        # Demand score
        .withColumn(
            "demand_score",
            round(
                (
                        col("search_signal") * lit(0.40)
                        + col("booking_signal") * lit(0.40)
                        + col("occupancy_signal") * lit(0.20)
                ),
                3
            )
        )

        # Price multiplier
        .withColumn(
            "price_multiplier",
            round(
                least(
                    greatest(
                        lit(1.0)
                        + col("demand_score") * lit(0.5),
                        lit(1.0)
                    ),
                    lit(1.5)
                ),
                2
            )
        )

        # Simulated price
        .withColumn(
            "simulated_price",
            round(
                col("average_booking_value")
                * col("price_multiplier"),
                2
            )
        )
    )


    # --------------------------------------------------------
    # Prepare data for Snowflake
    # --------------------------------------------------------

    pricing_df = (
        pricing_df

        # Ensure Snowflake TIMESTAMP compatibility
        .withColumn(
            "DEPARTURE_TIME",
            to_timestamp("DEPARTURE_TIME")
        )

        # Processing timestamp
        .withColumn(
            "PROCESSED_AT",
            current_timestamp()
        )
    )


    # --------------------------------------------------------
    # Select exactly the columns expected by Snowflake
    # --------------------------------------------------------

    final_pricing_df = pricing_df.select(
        "FLIGHT_ID",
        "TOTAL_SEATS",
        "DEPARTURE_TIME",
        "DAYS_TO_DEPARTURE",
        "BOOKING_COUNT",
        "SEATS_BOOKED",
        "OCCUPANCY",
        "SEARCH_COUNT",
        "SEARCH_SIGNAL",
        "BOOKING_REVENUE",
        "DEMAND_SCORE",
        "PRICE_MULTIPLIER",
        "SIMULATED_PRICE",
        "PROCESSED_AT"
    )


    # --------------------------------------------------------
    # Display pricing results
    # --------------------------------------------------------

    print()
    print("FINAL PRICING RESULTS")
    print("=" * 70)

    final_pricing_df.orderBy(
        col("DEMAND_SCORE").desc()
    ).show(
        30,
        truncate=False
    )


    # --------------------------------------------------------
    # Write pricing results to Snowflake
    # --------------------------------------------------------

    print()
    print("Writing pricing results to Snowflake...")

    (
        final_pricing_df.write
        .format("net.snowflake.spark.snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .option(
            "dbtable",
            "FLIGHT_PRICING"
        )
        .mode("append")
        .save()
    )

    print()
    print("Pricing results written to Snowflake successfully.")


# ============================================================
# Convert booking stream into pricing micro-batches
# ============================================================

pricing_query = (
    booking_metrics.writeStream
    .outputMode("complete")
    .foreachBatch(calculate_pricing)
    .option(
        "checkpointLocation",
        "/tmp/airline_pricing_checkpoint_v4"
    )
    .trigger(
        once=True
    )
    .start()
)


# ============================================================
# Wait for streaming query
# ============================================================

pricing_query.awaitTermination()


# ============================================================
# Stop Spark
# ============================================================

spark.stop()
