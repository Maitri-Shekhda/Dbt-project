from kafka import KafkaConsumer
import mysql.connector
import json
import time
from datetime import datetime

# Initialize buffer to hold partial data
data_buffer = {}

# MySQL DB connection
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="SR11**sa",
    database="website_traffic"
)

cursor = db.cursor()

# Kafka consumer setup
consumer = KafkaConsumer(
    'topic_pageviews',
    'topic_sessionduration',
    'topic_timeonpage',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='web-traffic-group'
)
try:
    print("Starting Kafka consumer. Press Ctrl+C to exit.")


    # Consume messages
    for message in consumer:
        topic = message.topic
        value = message.value
        record_id = value['record_id']
        
        # Add to buffer
        if record_id not in data_buffer:
            data_buffer[record_id] = {}

        if topic == 'topic_pageviews':
            data_buffer[record_id]['page_views'] = value['page_views']
        elif topic == 'topic_sessionduration':
            data_buffer[record_id]['session_duration'] = value['session_duration']
        elif topic == 'topic_timeonpage':
            data_buffer[record_id]['time_on_page'] = value['time_on_page']
        
        data_buffer[record_id]['timestamp'] = value['timestamp']

        # If all 3 fields are present, insert into DB
        if all(k in data_buffer[record_id] for k in ['page_views', 'session_duration', 'time_on_page']):
            try:
                row = data_buffer.pop(record_id)
                insert_query = """
                    INSERT INTO raw_traffic_data (page_views, session_duration, time_on_page, timestamp)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    row['page_views'],
                    row['session_duration'],
                    row['time_on_page'],
                    row['timestamp']
                ))
                db.commit()
                print(f"✅ Inserted record_id {record_id} into raw_traffic_data")
            except Exception as e:
                print(f"❌ Failed to insert record_id {record_id}: {e}")
except KeyboardInterrupt:
    print("Consumer stopped by user")
finally:
    consumer.close()