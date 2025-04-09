from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from datetime import datetime

# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("WebsiteTrafficAnalysis") \
    .config("spark.streaming.stopGracefullyOnShutdown",True)\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.jars", "mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Define schema for each topic
schema_page_views = StructType([
    StructField("page_views", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("record_id", IntegerType(), True)
])

schema_session_duration = StructType([
    StructField("session_duration", FloatType(), True),
    StructField("timestamp", StringType(), True),
    StructField("record_id", IntegerType(), True)
])

schema_time_on_page = StructType([
    StructField("time_on_page", FloatType(), True),
    StructField("timestamp", StringType(), True),
    StructField("record_id", IntegerType(), True)
])

# Read from Kafka topics
df_page_views = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_pageviews") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_page_views).alias("data")) \
    .select("data.*")

df_session_duration = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_sessionduration") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_session_duration).alias("data")) \
    .select("data.*")

df_time_on_page = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_timeonpage") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_time_on_page).alias("data")) \
    .select("data.*")

# Convert timestamp string to timestamp type
df_page_views = df_page_views.withColumn("timestamp", to_timestamp(col("timestamp")))
df_session_duration = df_session_duration.withColumn("timestamp", to_timestamp(col("timestamp")))
df_time_on_page = df_time_on_page.withColumn("timestamp", to_timestamp(col("timestamp")))

# Create watermarks to handle late data
df_page_views = df_page_views.withWatermark("timestamp", "1 minute")
df_session_duration = df_session_duration.withWatermark("timestamp", "1 minute")
df_time_on_page = df_time_on_page.withWatermark("timestamp", "1 minute")

# Register the streams as temporary views for SQL queries
df_page_views.createOrReplaceTempView("page_views_stream")
df_session_duration.createOrReplaceTempView("session_duration_stream")
df_time_on_page.createOrReplaceTempView("time_on_page_stream")

# Join the streams using SQL
joined_df = spark.sql("""
    SELECT 
        pv.record_id,
        pv.timestamp,
        pv.page_views,
        sd.session_duration,
        tp.time_on_page,
        (pv.page_views * 0.4) + (sd.session_duration / 60 * 0.3) + (tp.time_on_page / 60 * 0.3) AS engagement_score,
        CURRENT_TIMESTAMP() AS processing_time
    FROM page_views_stream pv
    INNER JOIN session_duration_stream sd
        ON pv.record_id = sd.record_id AND pv.timestamp = sd.timestamp
    INNER JOIN time_on_page_stream tp
        ON pv.record_id = tp.record_id AND pv.timestamp = tp.timestamp
""")

# Register the joined stream for further SQL analysis
joined_df.createOrReplaceTempView("joined_stream")


def process_batch(df, epoch_id):
    # Skip processing if no data
    if df.count() == 0:
        print(f"No data in batch {epoch_id}, skipping processing.")
        return
    
    # 1. First write metrics to MySQL
    # Add processing time as a float (seconds)
    df_with_processing_time = df.withColumn("processing_time", 
                      unix_timestamp(col("processing_time")) - unix_timestamp(col("timestamp")))
    
    # Select only the columns needed for streaming_metrics table
    metrics_df = df_with_processing_time.select(
        "record_id", "timestamp", "page_views", "session_duration", "time_on_page", "processing_time"
    )
    
    # Write to MySQL
    metrics_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/website_traffic") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "streaming_metrics") \
        .option("user", "root") \
        .option("password", "2004") \
        .mode("append") \
        .save()
    
    print(f"Metrics Batch {epoch_id}: {metrics_df.count()} records processed")
    
    # 2. Then process analytics with the same DataFrame directly
    from pyspark.sql.functions import window, count, sum, expr, hour, avg, corr, min, max, when, lit
    
    # 1. Page Views Distribution
    page_views_distribution = df \
        .withColumn("window", window("timestamp", "5 minutes")) \
        .groupBy("window", "page_views") \
        .agg(count("*").alias("count")) \
        .withColumn("percentage", expr("count * 100.0 / sum(count) over (partition by window)")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("page_views"),
            col("count"),
            col("percentage")
        )
    
    if page_views_distribution.count() > 0:
        page_views_distribution.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/website_traffic") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "page_views_distribution") \
            .option("user", "root") \
            .option("password", "2004") \
            .mode("append") \
            .save()
        print(f"Page Views Batch {epoch_id}: {page_views_distribution.count()} records processed")

    # 2. Session Categories
    session_categories = df \
        .withColumn("window", window("timestamp", "5 minutes")) \
        .withColumn("session_category", 
                   when(col("session_duration") < 60, "Short")
                   .when(col("session_duration") < 300, "Medium")
                   .otherwise("Long")) \
        .groupBy("window", "session_category") \
        .agg(count("*").alias("count")) \
        .withColumn("percentage", expr("count * 100.0 / sum(count) over (partition by window)")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("session_category"),
            col("count"),
            col("percentage")
        )
    
    if session_categories.count() > 0:
        session_categories.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/website_traffic") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "session_categories") \
            .option("user", "root") \
            .option("password", "2004") \
            .mode("append") \
            .save()
        print(f"Session Categories Batch {epoch_id}: {session_categories.count()} records processed")

    # 3. Engagement Score Stats
    engagement_scores = df \
        .withColumn("window", window("timestamp", "5 minutes")) \
        .groupBy("window") \
        .agg(
            avg("engagement_score").alias("avg_engagement_score"),
            min("engagement_score").alias("min_engagement_score"),
            max("engagement_score").alias("max_engagement_score")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_engagement_score"),
            col("min_engagement_score"),
            col("max_engagement_score")
        )
    
    if engagement_scores.count() > 0:
        engagement_scores.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/website_traffic") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "engagement_scores") \
            .option("user", "root") \
            .option("password", "2004") \
            .mode("append") \
            .save()
        print(f"Engagement Batch {epoch_id}: {engagement_scores.count()} records processed")

    # 4. Hourly Traffic Patterns (prints only)
    hourly_patterns = df \
        .groupBy(hour("timestamp").alias("hour_of_day")) \
        .agg(
            count("*").alias("visit_count"),
            avg("page_views").alias("avg_page_views"),
            avg("session_duration").alias("avg_session_duration"),
            avg("time_on_page").alias("avg_time_on_page"),
            avg("engagement_score").alias("avg_engagement")
        ) \
        .orderBy("hour_of_day")

    print("Hourly Traffic Patterns:")
    hourly_patterns.show()

    # 5. Correlation Analysis (prints only)
    correlation_analysis = df.select(
        corr("page_views", "session_duration").alias("corr_views_duration"),
        corr("page_views", "time_on_page").alias("corr_views_time"),
        corr("session_duration", "time_on_page").alias("corr_duration_time"),
        corr("page_views", "engagement_score").alias("corr_views_engagement"),
        corr("session_duration", "engagement_score").alias("corr_duration_engagement"),
        corr("time_on_page", "engagement_score").alias("corr_time_engagement")
    )

    print("Correlation Analysis:")
    correlation_analysis.show()

 #Combined streaming query for both metrics and analytics
combined_query = joined_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

# Wait for termination
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping all streaming queries...")
    for query in spark.streams.active:
        query.stop()
    print("Streaming application stopped")