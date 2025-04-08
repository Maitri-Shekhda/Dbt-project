from kafka import KafkaProducer
import json
import pandas as pd
from datetime import datetime, timedelta
import time

# Load dataset
df = pd.read_csv('updated_db.csv')  # Update this with the actual path
print(df)
# Reset index to ensure sequential order
#df = df.reset_index(drop=True)

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

count_page_views = 0
count_session_duration = 0
count_time_on_page = 0

# Loop through rows one at a time
for i, row in df.iterrows():
    try:
        # Use i (0, 1, 2, ...) to increment timestamp by 1 second
        current_timestamp = row["timestamp"]
        
        # Create messages with the same timestamp
        page_views_msg = {
            "page_views": int(row["Page Views"]),
            "timestamp": current_timestamp,
            "record_id": i+1
        }
        
        session_duration_msg = {
            'session_duration': float(row['Session Duration']),
            'timestamp': current_timestamp,
            'record_id': i+1
        }
        
        time_on_page_msg = {
            'time_on_page': float(row['Time on Page']),
            'timestamp': current_timestamp,
            'record_id': i+1
        }
        print("\n📤 Sending messages:")
        print("Page Views Msg:       ", page_views_msg)
        print("Session Duration Msg: ", session_duration_msg)
        print("Time on Page Msg:     ", time_on_page_msg)
        # Send messages to each topic
        producer.send('topic_pageviews', value=page_views_msg)
        count_page_views += 1

        producer.send('topic_sessionduration', value=session_duration_msg)
        count_session_duration += 1

        producer.send('topic_timeonpage', value=time_on_page_msg)
        count_time_on_page += 1

        # Optional delay to simulate streaming
        time.sleep(0.05)  # 10 ms

        if i % 100 == 0:
            print(f"✅ Processed record {i} with timestamp: {current_timestamp}")

    except Exception as e:
        print(f"❌ Error at record {i}: {e}")

# Flush messages
producer.flush()

# Final report
print(f"\n✅ Sent {count_page_views} messages to 'pageviews' topic")
print(f"✅ Sent {count_session_duration} messages to 'sessionduration' topic")
print(f"✅ Sent {count_time_on_page} messages to 'timeonpage' topic")