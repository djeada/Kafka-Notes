# Simple Dataflow Pipeline

This project provides a solution for bridging the gap between the Data Generator messaging system and the Kafka streaming platform. By integrating these systems, we aim to facilitate real-time data processing, ensure efficient storage, and maintain robust logging capabilities.

## System Components

### Data Generator Queue and Listener

- **Data Generator Message Broker**:
  - Set up the Data Generator message broker to manage incoming data efficiently.
  - Designate a specific queue within the Data Generator system dedicated to data ingestion.
  
- **Listener Application**:
  - Develop a custom application that ties to the Data Generator queue, monitoring incoming messages.
  - The listener's core function is to extract data from the Data Generator queue and relay it to the Kafka producer, setting the stage for the next phase of processing.

### Kafka Integration

- **Kafka Producer**:
  - This component is integrated with the listener application.
  - Its primary responsibility is to accept data from the Data Generator listener and transmit it to the specified Kafka topic.
  
- **Kafka Cluster**:
  - Represents a distributed system, consisting of several broker nodes, which assures data redundancy, fault-tolerance, and the capability to scale.
  - Within this cluster are various topics, one of which will be allocated to store messages originating from the Data Generator queue.

- **Kafka Topic**:
  - A specialized channel within the Kafka cluster where the producer dispatches messages.
  - This topic functions as the intermediary between the producer and consumers, making certain that data is readily accessible for real-time processing.

### Data Processing

- **Kafka Consumer**:
  - A software entity or service that taps into the Kafka cluster, subscribing to the specific topic containing messages from the Data Generator.
  - Its role is to retrieve messages from this topic and subject them to the stipulated processing logic.

- **Database Storage**:
  - Post message retrieval, the Kafka consumer can archive them in a suitable database.
  - Depending on the data's nature and expected query types, one might opt for a relational, NoSQL, or time-series database.

- **Logging**:
  - Integrated within the Kafka consumer, this mechanism ensures meticulous record-keeping.
  - Every operational nuance, potential errors, and message specifics are logged, paving the way for streamlined monitoring, debugging, and auditing.

```
+---------------------+      +------------------+      +-----------------+       
|                     |      |                  |      |                 |       
|      Message        |      |  Listener &      |      | Kafka Producer  |      
|       Broker        |      |  Kafka Producer  |      |                 |      
|                     |      |                  |      +-----------------+       
+--------+------------+      +---------+--------+                  |                      
         |                            |                            |                      
         |                            |                            |                      
         |                            |                            |                      
         v                            v                            v                      
 +-------+------+               +------+-------+            +------+--------+             
 |              |               |              |            |               |             
 |  Message     |               | Listener     |            | Kafka Topic   |             
 |   Queue      |   Messages    | Application  |    Send    |               |             
 |              +-------------->|              +----------->|               |             
 +--------------+               +--------------+            +-------+-------+             
                                                                    |                     
                                                                    |                     
                                                                    |                     
                                                                    v                     
                                                           +-------+--------+             
                                                           |                |             
                                                           | Kafka Consumer |             
                                                           |                |             
                                                           +-------+-------+              
                                                                   |                      
                                                                   |                      
                                                                   |                      
                                                                   v                      
                                                           +-------+-------+       
                                                           |               |       
                                                           |   Database    |       
                                                           |               |      
                                                           +---------------+

```



## Setup Guide

1. **Docker Network Setup**: 

    To ensure effective communication between Kafka and Zookeeper containers, establish a docker network named 'kafka-net'.
    ```
    sudo docker network create kafka-net
    ```

2. **Zookeeper Instance Initialization**:

    Launch a Zookeeper instance. This step is crucial for Kafka's operation.
    ```
    sudo docker run --name zookeeper --network=kafka-net -p 2181:2181 -e ZOOKEEPER_CLIENT_PORT=2181 -d confluentinc/cp-zookeeper:latest
    ```

3. **Kafka Instance Initialization**:

    With Zookeeper up, commence a Kafka instance. Ensure it connects to the running Zookeeper.

    ```
    sudo docker run --name kafka --network=kafka-net -p 9092:9092 -d --env KAFKA_BROKER_ID=1 --env KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka:latest
    ```
    

