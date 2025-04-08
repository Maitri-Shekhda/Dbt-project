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

# Function to write to MySQL streaming_metrics table
def write_to_streaming_metrics(df, epoch_id):
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
        .option("password", "SR11**sa") \
        .mode("append") \
        .save()
    
    print(f"Metrics Batch {epoch_id}: {metrics_df.count()} records processed")

# Write streaming metrics to MySQL
metrics_query = joined_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_streaming_metrics) \
    .start()

# Function to process analytics and write to MySQL
def process_analytics(df, epoch_id):
    if df.isEmpty():
        print(f"No data in batch {epoch_id}, skipping analytics.")
        return

    # Register the current batch as a temporary view
    df.createOrReplaceTempView("current_batch")

    spark = SparkSession.builder.getOrCreate()

    # 1. Page Views Distribution
    page_views_distribution = spark.sql("""
        WITH windowed_data AS (
            SELECT 
                window(timestamp, '5 minutes') AS window,
                page_views
            FROM current_batch
        )
        SELECT 
            window.start AS window_start,
            window.end AS window_end,
            page_views,
            COUNT(*) AS count,
            (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY window.start, window.end)) AS percentage
        FROM windowed_data
        GROUP BY window.start, window.end, page_views
    """)

    if not page_views_distribution.isEmpty():
        page_views_distribution.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/website_traffic") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "page_views_distribution") \
            .option("user", "root") \
            .option("password", "SR11**sa") \
            .mode("append") \
            .save()
        print(f"Page Views Batch {epoch_id}: {page_views_distribution.count()} records processed")

    # 2. Session Categories
    session_categories = spark.sql("""
        WITH windowed_data AS (
            SELECT 
                window(timestamp, '5 minutes') AS window,
                CASE 
                    WHEN session_duration < 60 THEN 'Short'
                    WHEN session_duration < 300 THEN 'Medium'
                    ELSE 'Long'
                END AS session_category
            FROM current_batch
        )
        SELECT 
            window.start AS window_start,
            window.end AS window_end,
            session_category,
            COUNT(*) AS count,
            (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY window.start, window.end)) AS percentage
        FROM windowed_data
        GROUP BY window.start, window.end, session_category
    """)

    if not session_categories.isEmpty():
        session_categories.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/website_traffic") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "session_categories") \
            .option("user", "root") \
            .option("password", "SR11**sa") \
            .mode("append") \
            .save()
        print(f"Session Categories Batch {epoch_id}: {session_categories.count()} records processed")

    # 3. Engagement Score Stats
    engagement_scores = spark.sql("""
        WITH windowed_data AS (
            SELECT 
                window(timestamp, '5 minutes') AS window,
                engagement_score
            FROM current_batch
        )
        SELECT 
            window.start AS window_start,
            window.end AS window_end,
            AVG(engagement_score) AS avg_engagement_score,
            MIN(engagement_score) AS min_engagement_score,
            MAX(engagement_score) AS max_engagement_score
        FROM windowed_data
        GROUP BY window.start, window.end
    """)

    if not engagement_scores.isEmpty():
        engagement_scores.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/website_traffic") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "engagement_scores") \
            .option("user", "root") \
            .option("password", "SR11**sa") \
            .mode("append") \
            .save()
        print(f"Engagement Batch {epoch_id}: {engagement_scores.count()} records processed")

    # 4. Hourly Traffic Patterns (prints only)
    hourly_patterns = spark.sql("""
        SELECT 
            HOUR(timestamp) AS hour_of_day,
            COUNT(*) AS visit_count,
            AVG(page_views) AS avg_page_views,
            AVG(session_duration) AS avg_session_duration,
            AVG(time_on_page) AS avg_time_on_page,
            AVG(engagement_score) AS avg_engagement
        FROM current_batch
        GROUP BY HOUR(timestamp)
        ORDER BY hour_of_day
    """)

    print("Hourly Traffic Patterns:")
    hourly_patterns.show()

    # 5. Correlation Metrics (prints only)
    correlation_analysis = spark.sql("""
        SELECT 
            CORR(page_views, session_duration) AS corr_views_duration,
            CORR(page_views, time_on_page) AS corr_views_time,
            CORR(session_duration, time_on_page) AS corr_duration_time,
            CORR(page_views, engagement_score) AS corr_views_engagement,
            CORR(session_duration, engagement_score) AS corr_duration_engagement,
            CORR(time_on_page, engagement_score) AS corr_time_engagement
        FROM current_batch
    """)

    print("Correlation Analysis:")
    correlation_analysis.show()

# Process analytics
analytics_query = joined_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_analytics) \
    .start()

# Wait for termination
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping all streaming queries...")
    for query in spark.streams.active:
        query.stop()
    print("Streaming application stopped")