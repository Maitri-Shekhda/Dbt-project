## Running Kafka and Creating Topics
### Step 1: Start ZooKeeper
Open CMD and run:

```cmd
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```
---

### Step 2: Start Kafka Broker
In a **new CMD window**, run:
```cmd
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
---
### Step 3: Create a Kafka Topic
In another **CMD window**, run:

```cmd
cd C:\kafka_2.13-3.5.1\bin\windows
kafka-topics.bat --create --topic topic_pageviews --bootstrap-server localhost:9092
kafka-topics.bat --create --topic topic_sessionduration --bootstrap-server localhost:9092
kafka-topics.bat --create --topic topic_timeonpage --bootstrap-server localhost:9092 
```
### Step 4: List All Topics

```cmd
kafka-topics.bat --list --bootstrap-server localhost:9092
```
---
