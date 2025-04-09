import pandas as pd
import mysql.connector

# Load CSV
df = pd.read_csv("updated_db.csv")

# Connect to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="root",       # Change this
    password="2004",   # Change this
    database="website_traffic"    # Change this
)
cursor = db.cursor()

# Optional: Create the table if it doesn't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_traffic_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    page_views VARCHAR(255),
    session_duration FLOAT,
    time_on_page FLOAT,
    timestamp DATETIME
)
""")
print(df.columns)
print(df.dtypes)

# Insert each row
for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO raw_traffic_data (page_views, session_duration, time_on_page, timestamp)
        VALUES (%s, %s, %s, %s)
    """, (row["Page Views"], row["Session Duration"], row["Time on Page"], row["timestamp"]))

db.commit()
cursor.close()
db.close()

print("CSV data inserted into MySQL successfully.")
