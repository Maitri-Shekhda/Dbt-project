from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

record_id = 2153

while True:
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Create messages for all 3 topics
    page_views_msg = {
        "page_views": random.randint(1, 10),
        "timestamp": current_timestamp,
        "record_id": record_id
    }

    session_duration_msg = {
        "session_duration": round(random.uniform(10, 600), 2),  # in seconds
        "timestamp": current_timestamp,
        "record_id": record_id
    }

    time_on_page_msg = {
        "time_on_page": round(random.uniform(5, 300), 2),  # in seconds
        "timestamp": current_timestamp,
        "record_id": record_id
    }

    # Send messages to respective topics
    producer.send('topic_pageviews', value=page_views_msg)
    producer.send('topic_sessionduration', value=session_duration_msg)
    producer.send('topic_timeonpage', value=time_on_page_msg)

    print(f" Sent record_id={record_id}")
    print(f"Page Views Msg:       {page_views_msg}")
    print(f"Session Duration Msg: {session_duration_msg}")
    print(f"Time on Page Msg:     {time_on_page_msg}")
    print("-" * 50)

    record_id += 1
    time.sleep(45)  # simulate real-time interval
