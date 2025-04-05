from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mysql.connector
from mysql.connector import Error
import time
import json
import os
os.environ["HADOOP_HOME"] = "C:\hadoop"
os.environ["SPARK_HOME"] = "C:\\spark-3.5.5"

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("WebsiteTrafficMonitoring") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# MySQL connection parameters
mysql_config = {
    'user': 'root',
    'password': 'SR11**sa',  # Replace with your MySQL password
    'host': 'localhost',
    'database': 'website_traffic',
    'raise_on_warnings': True
}

# Function to save data to MySQL
def save_to_mysql(df, epoch_id):
    # Start timing for performance measurement
    start_time = time.time()
    
    # Convert DataFrame to rows
    rows = df.collect()
    
    if not rows:
        print(f"Batch {epoch_id}: No data to save")
        return
    
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        
        # Insert data into streaming_metrics table
        for row in rows:
            query = """
            INSERT INTO streaming_metrics 
            (window_start, window_end, avg_page_views, avg_session_duration, avg_time_on_page, 
            total_records, processing_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            cursor.execute(query, (
                row['window_start'],
                row['window_end'],
                float(row['avg_page_views']),
                float(row['avg_session_duration']),
                float(row['avg_time_on_page']),
                int(row['count']),
                float(processing_time)
            ))
        
        # Commit the transaction
        conn.commit()
        print(f"Batch {epoch_id}: {len(rows)} records saved to MySQL. Processing time: {time.time() - start_time:.4f} seconds")
        
    except Error as e:
        print(f"Error saving to MySQL: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Define schema for each topic
page_views_schema = StructType([
    StructField("page_views", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("record_id", IntegerType(), True)
])

session_duration_schema = StructType([
    StructField("session_duration", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("record_id", IntegerType(), True)
])

time_on_page_schema = StructType([
    StructField("time_on_page", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("record_id", IntegerType(), True)
])

# Read from Kafka topics
page_views_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pageviews_topic") \
    .load() \
    .select(from_json(col("value").cast("string"), page_views_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("page_views", col("page_views").cast("int"))

session_duration_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sessionduration_topic")\
    .load() \
    .select(from_json(col("value").cast("string"), session_duration_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp")))

time_on_page_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "timeonpage_topic") \
    .load() \
    .select(from_json(col("value").cast("string"), time_on_page_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp")))

# Join the dataframes on record_id and timestamp
joined_df = page_views_df \
    .join(session_duration_df, ["record_id", "timestamp"]) \
    .join(time_on_page_df, ["record_id", "timestamp"])

# Process data with tumbling windows (15 minutes as per project requirements)
window_duration = "15 minutes"

# 1. Basic window aggregations
windowed_stats = joined_df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window(col("timestamp"), window_duration)) \
    .agg(
        avg("page_views").alias("avg_page_views"),
        avg("session_duration").alias("avg_session_duration"),
        avg("time_on_page").alias("avg_time_on_page"),
        min("page_views").alias("min_page_views"),
        max("page_views").alias("max_page_views"),
        min("session_duration").alias("min_session_duration"),
        max("session_duration").alias("max_session_duration"),
        min("time_on_page").alias("min_time_on_page"),
        max("time_on_page").alias("max_time_on_page"),
        count("*").alias("count")
    ) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

# 2. Page views distribution analysis
page_views_distribution = joined_df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(
        window(col("timestamp"), window_duration),
        col("page_views")
    ) \
    .count() \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

# 3. Session duration categorization
session_categories = joined_df \
    .withColumn("session_category", 
        when(col("session_duration") < 1, "Very Short")
        .when(col("session_duration") < 3, "Short")
        .when(col("session_duration") < 10, "Medium")
        .otherwise("Long")
    ) \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(
        window(col("timestamp"), window_duration),
        col("session_category")
    ) \
    .count() \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

# 4. Engagement score calculation (combining metrics)
engagement_score = joined_df \
    .withColumn("engagement_score", 
        col("page_views") * 0.4 + col("session_duration") * 0.3 + col("time_on_page") * 0.3
    ) \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window(col("timestamp"), window_duration)) \
    .agg(
        avg("engagement_score").alias("avg_engagement_score"),
        min("engagement_score").alias("min_engagement_score"),
        max("engagement_score").alias("max_engagement_score")
    ) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

# Print output to console for debugging
print_query1 = windowed_stats \
    .writeStream \
   .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print_query2 = page_views_distribution \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print_query3 = session_categories \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print_query4 = engagement_score \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Save basic stats to MySQL
mysql_query = windowed_stats \
    .writeStream \
    .foreachBatch(save_to_mysql) \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()