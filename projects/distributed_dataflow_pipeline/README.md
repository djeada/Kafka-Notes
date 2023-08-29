# Distributed Dataflow Pipeline

The distributed dataflow pipeline, by harnessing the power of multiprocessing, facilitates efficient and parallel processing of vast data streams. With proper configuration and monitoring, it offers a robust solution for high-throughput data scenarios.

## System Components

### Multiprocessed Data Ingestion

- **Data Ingestor**:
  - Functions as the primary interface for incoming data streams.
  - Utilizes multiprocessing to spawn `n` child processes that independently pull data from sources.

- **Buffer Queue**:
  - A multiprocessing-safe queue that acts as a temporary storage, absorbing data from the ingestors.
  - Guarantees thread safety and supports concurrent pushes and pulls from multiple processes.

### Multiprocessed Data Processing

- **Data Processor**:
  - Leverages multiprocessing to initiate `n` worker processes.
  - Each worker is responsible for pulling data from the buffer queue, processing it, and then pushing the result to the next phase.

- **Intermediate Storage**:
  - An optional transient storage mechanism (like an in-memory database or a cache) that temporarily holds processed data.
  - Ensures that the processing components don't become bottlenecks.

### Multiprocessed Data Storage

- **Data Storage Workers**:
  - Using multiprocessing, `n` worker processes are launched, each responsible for persistent data storage.
  - Each worker fetches processed data either from the intermediate storage or directly from the processing queue and persists it to the final database or file storage.

### Logging and Monitoring

- **Log Aggregator**:
  - A component that collates logs from all worker processes.
  - Responsible for centralizing logs and ensuring that they are written to a persistent store or displayed in real-time, aiding in debugging and performance tuning.

- **Metrics Collector**:
  - Actively monitors each process's health, resource utilization, and other crucial metrics.
  - Aids in identifying bottlenecks, ensuring efficient resource utilization, and enabling the system's smooth scaling.


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
    

