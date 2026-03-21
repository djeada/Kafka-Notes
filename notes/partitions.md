## Kafka Topics and Partitions

Topics and partitions form the backbone of Kafka's data organization. A topic is a logical category for messages, while partitions allow topics to be split across multiple brokers for parallelism and scalability.

```
+------------------------------- Kafka Topic "orders" --------------------------------+
|                                                                                     |
|  +--- Partition 0 ---+   +--- Partition 1 ---+   +--- Partition 2 ---+              |
|  | Offset 0: msg_a   |   | Offset 0: msg_d   |   | Offset 0: msg_g   |              |
|  | Offset 1: msg_b   |   | Offset 1: msg_e   |   | Offset 1: msg_h   |              |
|  | Offset 2: msg_c   |   | Offset 2: msg_f   |   | Offset 2: msg_i   |              |
|  +--------------------+   +--------------------+   +--------------------+             |
+-------------------------------------------------------------------------------------+
```

### How Topics Work

1. **Logical Grouping**:
   - A topic is a named feed to which records are published. Think of it as a database table or a folder in a filesystem.

2. **Append-Only Log**:
   - Each partition within a topic is an ordered, immutable sequence of records. New messages are always appended at the end.

3. **Retention Policy**:
   - Messages in a topic are retained for a configurable period (e.g., 7 days) or until a size limit is reached. Log compaction can also be enabled to keep only the latest value per key.

### How Partitions Work

1. **Parallel Processing**:
   - Partitions allow a topic's data to be distributed across multiple brokers. Each partition can be consumed independently, enabling parallelism.

2. **Ordering Guarantees**:
   - Messages within a single partition are strictly ordered by offset. There is no ordering guarantee across partitions.

3. **Key-Based Routing**:
   - When a message key is provided, Kafka hashes the key to determine the target partition. Messages with the same key always land in the same partition, preserving order for related events.

4. **Round-Robin Assignment**:
   - When no key is provided, Kafka distributes messages across partitions in a round-robin fashion to balance the load evenly.

### The Single Writer Pattern

The Single Writer pattern ensures that only one entity writes or processes data at a time to avoid conflicts and maintain consistency. Kafka applies this concept at the partition level -- only one consumer in a consumer group processes each partition at a time.

```
Consumer Group "order-processing"

  +--- Consumer 1 ---+     +--- Consumer 2 ---+     +--- Consumer 3 ---+
  |  Partition 0     |     |  Partition 1     |     |  Partition 2     |
  +------------------+     +------------------+     +------------------+
```

Each consumer processes all messages from its assigned partition in order, which keeps the sequence right and avoids race conditions.

### Real-World Example: E-commerce Order Events

Imagine an e-commerce platform receiving events like:

- `OrderPlaced`
- `OrderConfirmed`
- `OrderShipped`

All events for a specific order are routed to the same partition using the order ID as the message key, ensuring they are processed in order without needing a separate partition for every order.

One consumer processes the events in the correct sequence, so the system doesn't ship an order before confirming it.

### Choosing the Right Number of Partitions

1. **More Partitions = More Parallelism**:
   - The number of partitions limits the maximum number of consumers that can read from a topic in parallel. If you have 10 partitions, at most 10 consumers in a group can work simultaneously.

2. **Avoid Over-Partitioning**:
   - Each partition consumes resources: file handles, memory for index segments, and replication overhead. A common starting point is to match the number of partitions to the expected peak consumer count.

3. **Rebalancing Cost**:
   - Adding or removing partitions triggers a consumer group rebalance, temporarily pausing consumption. Plan the partition count ahead of time to minimize rebalances.

4. **Disk and Network**:
   - Each partition maintains its own log segment on disk. Very large partition counts can increase end-to-end latency during leader elections and replication.

### Partition Rebalancing and Fault Tolerance

1. **Consumer Failure**:
   - If a consumer fails, Kafka reassigns its partitions to another consumer in the group. The new consumer picks up from the last committed offset, keeping the order intact and avoiding duplicated work.

2. **Broker Failure**:
   - If a broker holding a partition leader goes down, a follower replica from the ISR (In-Sync Replica) set is promoted to leader. Producers and consumers are redirected to the new leader automatically.

3. **Partition Reassignment**:
   - When brokers are added to or removed from a cluster, partitions can be manually or automatically reassigned using the `kafka-reassign-partitions.sh` tool to rebalance the load.

### Common Pitfalls

1. **Hot Partitions**:
   - If message keys are not evenly distributed, some partitions may receive far more data than others. Monitor partition sizes and choose keys that distribute evenly.

2. **Too Few Partitions**:
   - Under-partitioned topics limit parallelism and create bottlenecks. The partition count cannot be decreased after creation; only increased.

3. **Too Many Partitions**:
   - Over-partitioning increases metadata overhead, memory usage, and leader election time. Aim for a balance between parallelism and resource consumption.
