from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    count,
    max,
    lit,
    round,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    IntegerType,
    StringType,
)

spark = (
    SparkSession.builder
    .appName("AirlineSearchDemand")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

search_schema = StructType([
    StructField("search_id", LongType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("flight_id", IntegerType(), True),
    StructField("searched_at", StringType(), True),
])

searches_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "airline_searches")
    .option("startingOffsets", "earliest")
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
    .filter(col("flight_id").isNotNull())
)

search_metrics = (
    searches
    .groupBy("flight_id")
    .agg(
        count("*").alias("search_count")
    )
)

def calculate_search_demand(batch_df, batch_id):

    print()
    print("=" * 70)
    print(f"SEARCH DEMAND BATCH: {batch_id}")
    print("=" * 70)

    # Find the highest search count in this batch.
    max_searches = batch_df.agg(
        max("search_count")
    ).collect()[0][0]

    if max_searches is None or max_searches == 0:
        print("No search data found.")
        return

    result = (
        batch_df
        .withColumn(
            "search_signal",
            round(
                col("search_count")
                / lit(float(max_searches)),
                3
            )
        )
        .select(
            "flight_id",
            "search_count",
            "search_signal"
        )
        .orderBy(
            col("search_signal").desc()
        )
    )

    result.write.mode("overwrite").parquet(
        "/tmp/airline_search_metrics"
    )

    result.show(
        30,
        truncate=False
    )

search_query = (
    search_metrics.writeStream
    .outputMode("complete")
    .foreachBatch(calculate_search_demand)
    .option(
        "checkpointLocation",
        "/tmp/airline_search_demand_checkpoint_v2"
    )
    .trigger(once=True)
    .start()
)

search_query.awaitTermination()

print("SEARCH STREAM FINISHED")

spark.stop()
