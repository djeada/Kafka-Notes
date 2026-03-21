## Kafka Brokers

Brokers are the fundamental building blocks of a Kafka cluster and play a critical role in ensuring message durability, scalability, and fault tolerance. Let's delve deeper into the details of Kafka brokers:

```
+----------------------- Broker 0 -----------------------+
|                                                        |
|  +--- Topic A ---+  +--- Topic A ---+  +--- Topic B --+--+
|  | Partition 0   |  | Partition 1   |  | Partition 0   |  |
|  | (Leader)      |  | (Follower)    |  | (Follower)    |  |
|  +---------------+  +---------------+  +---------------+  |
|                                                            |
|  +--- Topic B ---+  +--- Topic C ---+                     |
|  | Partition 1   |  | Partition 0   |                     |
|  | (Leader)      |  | (Follower)    |                     |
|  +---------------+  +---------------+                     |
+-----------------------------------------------------------+

+----------------------- Broker 1 -----------------------+
|                                                        |
|  +--- Topic A ---+  +--- Topic A ---+  +--- Topic B --+--+
|  | Partition 0   |  | Partition 1   |  | Partition 0   |  |
|  | (Follower)    |  | (Leader)      |  | (Leader)      |  |
|  +---------------+  +---------------+  +---------------+  |
|                                                            |
|  +--- Topic B ---+  +--- Topic C ---+                     |
|  | Partition 1   |  | Partition 0   |                     |
|  | (Follower)    |  | (Leader)      |                     |
|  +---------------+  +---------------+                     |
+-----------------------------------------------------------+

+----------------------- Broker 2 -----------------------+
|                                                        |
|  +--- Topic A ---+  +--- Topic A ---+  +--- Topic B --+--+
|  | Partition 0   |  | Partition 1   |  | Partition 0   |  |
|  | (Follower)    |  | (Follower)    |  | (Follower)    |  |
|  +---------------+  +---------------+  +---------------+  |
|                                                            |
|  +--- Topic B ---+  +--- Topic C ---+                     |
|  | Partition 1   |  | Partition 0   |                     |
|  | (Follower)    |  | (Follower)    |                     |
|  +---------------+  +---------------+                     |
+-----------------------------------------------------------+

Replication flow (replication factor = 3):

  Topic A, Partition 0:  Broker 0 (Leader) --replicate--> Broker 1, Broker 2
  Topic A, Partition 1:  Broker 1 (Leader) --replicate--> Broker 0, Broker 2
  Topic B, Partition 0:  Broker 1 (Leader) --replicate--> Broker 0, Broker 2
  Topic B, Partition 1:  Broker 0 (Leader) --replicate--> Broker 1, Broker 2
  Topic C, Partition 0:  Broker 1 (Leader) --replicate--> Broker 0, Broker 2
```

### Basics of How Brokers Work

1. **Storage Units**: 
   - Brokers are essentially storage units that store data and serve client requests. Each broker holds a set of topic partitions.

2. **Cluster Metadata Management**:
   - Historically, brokers relied on ZooKeeper to maintain cluster state (broker registration, topic metadata, partition assignments). Starting with Kafka 3.3, the **KRaft (Kafka Raft)** mode replaces ZooKeeper with an internal Raft-based consensus protocol, simplifying the architecture and removing the external dependency.

3. **Request Handlers**:
   - Brokers handle data produce and consume requests from clients. Producers write data to topics, and consumers read from them. Each broker runs a configurable number of network and I/O threads to handle requests concurrently.

4. **Node Identifier**:
   - Every broker in a Kafka cluster has a unique ID which is either set manually via configuration or generated automatically.

5. **Controller**:
   - One broker in the cluster acts as the controller. The controller is responsible for partition leader election, reassigning partitions when brokers join or leave, and managing topic creation and deletion. In KRaft mode, a dedicated set of controller nodes can be configured separately from data brokers.

### KRaft Mode Deep Dive

KRaft (Kafka Raft) is the consensus protocol that replaces ZooKeeper for cluster metadata management. It was marked production-ready in Kafka 3.3 and became the default in Kafka 3.5+.

1. **How KRaft Works**:
   - KRaft uses a Raft-based quorum of controller nodes to manage cluster metadata. One controller is elected as the **active controller** (leader), while the others are hot standbys. Metadata changes are committed via Raft consensus, ensuring a majority of controllers agree before a change takes effect.

2. **Controller Quorum Configuration**:
   - The quorum is defined via `controller.quorum.voters`, a comma-separated list of controller node IDs and their addresses:
     - `controller.quorum.voters=0@controller-0:9093,1@controller-1:9093,2@controller-2:9093`
   - An odd number of controllers (3 or 5) is recommended to maintain a clear majority for leader election.

3. **Metadata Topic (`__cluster_metadata`)**:
   - All cluster metadata (broker registrations, topic configurations, partition assignments, ACLs) is stored in an internal topic called `__cluster_metadata`. This topic is replicated across all controller nodes via the Raft protocol and serves as the single source of truth for the cluster state.
   - Data brokers fetch metadata updates from the active controller through a metadata log, similar to how followers replicate data partitions.

4. **Combined vs Dedicated Controller Nodes**:
   - **Combined mode**: A node acts as both a controller and a data broker (`process.roles=broker,controller`). Suitable for small clusters and development environments.
   - **Dedicated mode**: Controller nodes handle only metadata (`process.roles=controller`), while separate nodes serve as data brokers (`process.roles=broker`). Recommended for production, as it isolates controller workload from data-plane traffic.

5. **Migration from ZooKeeper to KRaft**:
   - Kafka provides a bridge release migration path. The high-level steps are:
     - Deploy new KRaft controller nodes alongside the existing ZooKeeper-based cluster.
     - Migrate the cluster metadata from ZooKeeper to the KRaft controllers using the `kafka-metadata.sh` tool.
     - Reconfigure each broker to point to the KRaft controllers and restart them in a rolling fashion.
     - Once all brokers are running in KRaft mode, decommission the ZooKeeper ensemble.
   - During migration, the cluster remains fully operational with no downtime.

### How to Configure and Scale Brokers

1. **Configuration**:
   - Kafka brokers are configured using a properties file (`server.properties`).
   - Essential configurations include `broker.id`, `log.dirs`, and `zookeeper.connect` (or `controller.quorum.voters` in KRaft mode).
   - Other important settings:
     - `num.partitions`: Default number of partitions for auto-created topics.
     - `default.replication.factor`: Default replication factor for auto-created topics.
     - `log.retention.hours` / `log.retention.bytes`: How long or how much data to keep.
     - `num.network.threads` and `num.io.threads`: Thread pool sizes for handling requests.
     - `min.insync.replicas`: Minimum ISR count required for a write to succeed when `acks=all`.

2. **Scalability**:
   - **Horizontal Scaling**: Kafka supports horizontal scaling by simply adding more brokers to a cluster. Partitions can be reassigned to new brokers using the Kafka reassignment tool (`kafka-reassign-partitions.sh`).
   - **Vertical Scaling**: Increasing the hardware resources (CPU, Memory, Storage) of existing brokers is another way, though horizontal scaling is often preferred due to Kafka's distributed nature.

3. **Network & Disk Throughput**:
   - Proper broker operation demands adequate disk I/O and network throughput, especially in heavy traffic scenarios. Monitoring and tuning these parameters are crucial.
   - Use dedicated disks for Kafka log directories to avoid I/O contention with the operating system.
   - Consider using XFS as the filesystem for Kafka log directories, as it generally offers better performance than ext4 for Kafka workloads.

### Role of Brokers in Replication

1. **Data Redundancy**:
   - Kafka provides data redundancy by replicating topic partitions across multiple brokers.
   
2. **Leader and Follower**:
   - For each partition, one broker serves as the leader, while the others serve as followers. All write and read requests go through the leader, and then data is replicated to the followers.

3. **In-Sync Replica (ISR)**:
   - The set of replicas which are fully caught up with the leader are termed as In-Sync Replicas (ISRs). A write to a partition isn't considered complete until it has been written to its leader and a configurable number of followers.

4. **Handling Broker Failures**:
   - If a broker (and hence the leader replica for some partitions) fails, a follower replica (from the ISR list) is elected as the new leader, ensuring data availability.
   - Replication ensures that data is safe and available even in the face of broker failures.

### Inter-Broker Communication

Brokers constantly communicate with each other to replicate data and maintain cluster consistency.

1. **Replication Protocol**:
   - Follower replicas send **Fetch requests** to the leader of each partition they replicate, pulling new messages in batches. This is the same Fetch API that consumers use, but followers issue requests with their replica ID to identify themselves.
   - The leader tracks the fetch progress of each follower to determine which replicas are in sync.

2. **High Watermark (HW)**:
   - The high watermark is the offset of the last message that has been successfully replicated to all ISR members. Consumers can only read up to the high watermark, ensuring they never see messages that could be lost if the leader fails.
   - The leader advances the high watermark once all ISR followers have acknowledged a given offset.

```
  Leader Partition Log:

  | msg 0 | msg 1 | msg 2 | msg 3 | msg 4 | msg 5 |
  |-------|-------|-------|-------|-------|-------|
                                ^               ^
                                |               |
                          High Watermark   Log End Offset
                         (committed,        (latest written,
                          visible to         not yet fully
                          consumers)         replicated)
```

3. **Leader Epoch**:
   - A **leader epoch** is a monotonically increasing number assigned each time a new leader is elected for a partition. It prevents stale leaders (e.g., a broker that was briefly partitioned) from overwriting data on followers with outdated messages.
   - Followers include the leader epoch in their Fetch requests so the leader can detect and reject stale fetches.

4. **Replica Lag and ISR Shrinking**:
   - If a follower falls behind the leader by more than `replica.lag.time.max.ms` (default 30 seconds), the leader removes it from the ISR set. The follower continues fetching and is re-added to the ISR once it catches up.

### Log Management

Kafka brokers store messages in an append-only commit log on disk. Understanding log structure and retention is critical for capacity planning.

1. **Log Segments**:
   - Each partition's log is split into **segments**. The active segment receives new writes; older segments are immutable and eligible for cleanup.
   - A new segment is rolled when the current one reaches `log.segment.bytes` (default 1 GB) or after `log.roll.ms` / `log.roll.hours` elapses, whichever comes first.
   - Each segment consists of a `.log` file (message data), a `.index` file (offset-to-position mapping), and a `.timeindex` file (timestamp-to-offset mapping).

```
  Partition 0 Log Directory:

  00000000000000000000.log        <-- oldest segment
  00000000000000000000.index
  00000000000000000000.timeindex
  00000000000000524288.log        <-- next segment (starts at offset 524288)
  00000000000000524288.index
  00000000000000524288.timeindex
  00000000000001048576.log        <-- active segment (currently being written)
  00000000000001048576.index
  00000000000001048576.timeindex
```

2. **Log Retention (Delete Policy)**:
   - When `cleanup.policy=delete` (the default), Kafka removes old segments based on time or size:
     - `log.retention.hours` / `log.retention.ms`: Maximum age of a segment before deletion (default 168 hours / 7 days).
     - `log.retention.bytes`: Maximum size of a partition's log before the oldest segments are deleted. Set to `-1` (default) for no size limit.
   - Retention is evaluated per-partition, and only closed (non-active) segments are eligible for deletion.

3. **Log Compaction (Compact Policy)**:
   - When `cleanup.policy=compact`, Kafka retains only the **latest value for each message key** within a partition. Older records with the same key are removed during background compaction.
   - This is ideal for changelog or state-snapshot topics (e.g., database CDC, KTable changelogs) where only the current state matters.
   - A message with a `null` value acts as a **tombstone**, marking the key for deletion after a configurable delay (`delete.retention.ms`).
   - Both policies can be combined: `cleanup.policy=compact,delete` compacts first, then deletes segments that exceed the retention window.

4. **Key Configuration Summary**:

   | Parameter | Default | Description |
   |---|---|---|
   | `log.segment.bytes` | 1 GB | Max size of a single log segment |
   | `log.roll.ms` / `log.roll.hours` | 168 hours | Time before rolling a new segment |
   | `log.retention.hours` | 168 hours | Time-based retention for delete policy |
   | `log.retention.bytes` | -1 (unlimited) | Size-based retention per partition |
   | `log.cleanup.policy` | delete | `delete`, `compact`, or `compact,delete` |
   | `log.cleaner.min.cleanable.ratio` | 0.5 | Min dirty ratio to trigger compaction |
   | `delete.retention.ms` | 86400000 (24h) | How long tombstones are retained after compaction |

### Broker Performance Tuning

Tuning broker performance is essential for handling high-throughput workloads.

1. **JVM Settings**:
   - Kafka runs on the JVM, so heap size and garbage collection settings directly impact performance.
   - **Heap size**: A heap of 6-8 GB is typically recommended. Kafka relies heavily on the OS page cache for reads, so leaving ample memory for the page cache (outside the JVM heap) is more important than a large heap.
   - **Garbage Collection**: Use the G1 garbage collector (`-XX:+UseG1GC`) for its predictable pause times. Key G1 tuning flags:
     - `-XX:MaxGCPauseMillis=20`
     - `-XX:InitiatingHeapOccupancyPercent=35`

2. **Network and I/O Threads**:
   - `num.network.threads` (default 3): Number of threads handling network requests (reading from and writing to the socket). Increase for clusters with many concurrent client connections.
   - `num.io.threads` (default 8): Number of threads performing disk I/O (reading and writing log segments). Should be at least equal to the number of disks used for log directories.
   - `queued.max.requests` (default 500): Maximum number of queued requests before blocking network threads. Acts as a backpressure mechanism.

3. **Socket Buffer Sizes**:
   - `socket.send.buffer.bytes` (default 102400): SO_SNDBUF size for broker socket connections. Increase to 1 MB or more for high-throughput cross-datacenter replication.
   - `socket.receive.buffer.bytes` (default 102400): SO_RCVBUF size. Match this to the send buffer for symmetric throughput.
   - Setting either to `-1` uses the OS default.

4. **Filesystem Recommendations**:
   - **XFS** is the recommended filesystem for Kafka log directories. It handles large sequential writes and deletes efficiently and avoids performance cliffs seen with ext4 under heavy write loads.
   - Mount with `noatime` to reduce unnecessary inode updates on reads.
   - Avoid swap by setting `vm.swappiness=1` on the OS level, as swapping severely degrades broker latency.

5. **OS-Level Tuning**:
   - Increase the file descriptor limit (`ulimit -n`) to at least 100,000, since Kafka opens many segment and index files.
   - Set `vm.dirty_ratio` and `vm.dirty_background_ratio` to lower values (e.g., 5 and 1) to flush dirty pages to disk more frequently, reducing the risk of long I/O stalls.

### Quotas and Multi-Tenancy

Kafka supports resource quotas to prevent any single client from monopolizing broker resources, enabling safe multi-tenant deployments.

1. **Produce and Fetch Byte-Rate Quotas**:
   - `producer_byte_rate`: Maximum bytes per second a producer client can publish to the broker.
   - `consumer_byte_rate`: Maximum bytes per second a consumer client can fetch from the broker.
   - When a client exceeds its quota, the broker delays responses (throttles) rather than rejecting requests, smoothing out traffic spikes.

2. **Request Percentage Quotas**:
   - `request_percentage`: Limits the percentage of broker I/O and network threads a client can consume. This protects against clients that issue a large number of small requests, which might not trigger byte-rate quotas but still overwhelm broker threads.

3. **Quota Scopes**:
   - Quotas can be applied at three levels of granularity:
     - **User principal**: Based on the authenticated user identity (e.g., SASL principal).
     - **Client ID**: Based on the `client.id` set by the producer or consumer.
     - **User + Client ID**: The most specific scope, applying to a particular user using a particular client ID.
   - A default quota can be set for all users or client IDs, with overrides for specific ones.

4. **Configuring Quotas**:
   - Quotas are configured dynamically using `kafka-configs.sh` without requiring a broker restart:
     - Per user: `kafka-configs.sh --alter --add-config 'producer_byte_rate=1048576,consumer_byte_rate=2097152' --entity-type users --entity-name my-user`
     - Per client ID: `kafka-configs.sh --alter --add-config 'producer_byte_rate=1048576' --entity-type clients --entity-name my-client-id`
     - Default for all users: `kafka-configs.sh --alter --add-config 'producer_byte_rate=1048576' --entity-type users --entity-default`

### Rack Awareness

Kafka supports rack-aware replica placement to improve fault tolerance across physical failure domains (racks, availability zones, or data centers).

1. **Configuring Rack ID**:
   - Each broker is assigned a rack identifier via the `broker.rack` property in `server.properties`:
     - `broker.rack=us-east-1a`
   - When rack awareness is enabled, Kafka's partition assignment algorithm distributes replicas across different racks so that no single rack failure takes down all replicas of any partition.

2. **How Replica Placement Works**:
   - During topic creation or partition reassignment, the controller places replicas using a round-robin strategy that alternates across racks. For a topic with replication factor 3 across 3 racks:

```
  Rack A          Rack B          Rack C
  +----------+    +----------+    +----------+
  | Broker 0 |    | Broker 1 |    | Broker 2 |
  |          |    |          |    |          |
  | P0 Lead  |    | P0 Foll  |    | P0 Foll  |
  | P1 Foll  |    | P1 Lead  |    | P1 Foll  |
  | P2 Foll  |    | P2 Foll  |    | P2 Lead  |
  +----------+    +----------+    +----------+
```

3. **Fault Tolerance Benefit**:
   - Without rack awareness, all replicas of a partition could land on brokers in the same rack. If that rack loses power or network, all replicas become unavailable and data is at risk.
   - With rack awareness, losing an entire rack still leaves at least one replica available in another rack, preserving both availability and durability.

4. **Cloud Availability Zones**:
   - In cloud deployments (AWS, GCP, Azure), set `broker.rack` to the availability zone (e.g., `us-east-1a`, `us-east-1b`). This ensures replicas are spread across AZs, protecting against zone-level outages.

### Broker Maintenance and Troubleshooting

1. **Rolling Restarts**:
   - When updating configuration or upgrading Kafka, perform rolling restarts one broker at a time. This keeps the cluster available while each broker is briefly taken offline.

2. **Unclean Leader Election**:
   - By default (`unclean.leader.election.enable=false`), Kafka only elects leaders from the ISR set. Enabling unclean leader election allows out-of-sync replicas to become leaders, which can cause data loss but improves availability.

3. **Disk Failure**:
   - If a disk holding log directories fails, the broker marks those partitions as offline. Configure multiple `log.dirs` across different disks to limit the blast radius of a single disk failure.

4. **Monitoring Broker Health**:
   - Track key metrics such as `UnderReplicatedPartitions`, `ActiveControllerCount`, `OfflinePartitionsCount`, and request latency. A healthy cluster should have zero under-replicated and offline partitions.

By understanding the core functionalities and configurations of Kafka brokers, you can effectively set up, scale, and manage your Kafka cluster with resilience and efficiency.
