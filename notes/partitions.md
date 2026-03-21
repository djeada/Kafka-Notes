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

### Log Segments Within Partitions

Each partition on disk is a directory of log segment files. Understanding segments is key to retention, recovery, and performance.

1. **Segment Files**:
   - A partition directory (e.g., `orders-0/`) holds `.log` files named by their base offset. Only the latest segment, called the **active segment**, accepts new writes. Older segments are immutable.
2. **Segment Rolling**:
   - A new segment is created when the active segment hits a size or time threshold:
     - `log.segment.bytes` (default 1 GB) -- maximum size of a single segment file.
     - `log.roll.ms` / `log.roll.hours` (default 7 days) -- maximum age before rolling.
3. **Index Files**:
   - Each segment has two companion index files that enable fast offset lookups:
     - `.index` -- maps logical offsets to physical file positions in the `.log` file.
     - `.timeindex` -- maps timestamps to offsets for time-based seeks.
4. **Log Cleanup Policies**:
   - `delete` -- removes segments older than `log.retention.hours` or exceeding `log.retention.bytes`.
   - `compact` -- keeps only the latest record per key. Useful for changelog-style topics.

```
Partition 0 Directory: /var/kafka-logs/orders-0/
+--------------------------------------------------------------------+
|  +-- Segment 0 --+  +-- Segment 453 --+  +-- Active Seg 912 --+   |
|  | .log           |  | .log            |  | .log  <-- writes   |   |
|  | .index         |  | .index          |  | .index             |   |
|  | .timeindex     |  | .timeindex      |  | .timeindex         |   |
|  +----------------+  +-----------------+  +--------------------+   |
+--------------------------------------------------------------------+

Inside a .log segment:
+----------+----------+----------+-----+----------+
| Offset 0 | Offset 1 | Offset 2 | ... | Offset N |
| key=A    | key=B    | key=A    |     | key=B    |
+----------+----------+----------+-----+----------+

Corresponding .index (sparse -- not every offset is indexed):
+--------------------+----------------------+-----+
| Offset 0 -> pos 0  | Offset 4 -> pos 782  | ... |
+--------------------+----------------------+-----+
```

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

### Custom Partitioners

By default, the Kafka producer decides which partition a record lands in. You can override this with a custom partitioner.

1. **Default Partitioner Behavior**:
   - When a key is present, the default partitioner computes `murmur2(key) % num_partitions` to select the partition. This is deterministic -- the same key always maps to the same partition as long as the partition count stays the same.
   - When the key is `null`, modern clients use the **sticky partitioner**, which sends all records in a batch to one partition, then picks a new one for the next batch. This improves batching over older round-robin.
2. **When to Build a Custom Partitioner**:
   - You need geographic or tenant-based routing (e.g., route EU orders to partitions 0-2).
   - The default hash produces hot partitions because the key space is skewed.
   - You want to co-locate related keys that do not share a common prefix.

3. **Python Example** (using `confluent-kafka`):

```python
import random
from confluent_kafka import Producer

def region_partitioner(key, all_partitions, available_partitions):
    """Route messages to partitions based on region prefix."""
    if key is None:
        return random.choice(available_partitions)
    region = key.decode("utf-8").split("-")[0]
    region_map = {"us": 0, "eu": 1, "ap": 2}
    partition = region_map.get(region, 0)
    return partition if partition in available_partitions else available_partitions[0]

producer = Producer({"bootstrap.servers": "localhost:9092"})
target = region_partitioner(b"eu-order-42", list(range(3)), list(range(3)))
producer.produce(topic="orders", key="eu-order-42", value="order data", partition=target)
producer.flush()
```

### Strategies for Choosing Partition Keys

1. **Aim for Even Distribution**:
   - Choose keys with high cardinality (e.g., user IDs, order IDs) so the murmur2 hash spreads records evenly.
2. **Avoid Hot Keys**:
   - A small set of very active keys funnels traffic into one partition. Consider adding a random suffix (`userId-<shard>`) and reconciling downstream.
3. **Compound Keys**:
   - Combine fields (e.g., `tenantId-deviceId`) when ordering needs span multiple dimensions while still distributing across tenants.
4. **Null Keys**:
   - When ordering does not matter, omit the key. The sticky partitioner batches records to one partition per produce call, then rotates.
5. **Beware of Partition Count Changes**:
   - Adding partitions changes `hash(key) % num_partitions`. Records for the same key may land in a different partition.

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

### Partition Leader Election

Every partition has one leader replica and zero or more follower replicas. The leader handles all reads and writes; followers replicate data to stay in sync.

1. **Preferred Leader**:
   - Kafka designates a **preferred replica** (first in the assignment list) as the leader at topic creation. Broker restarts or failures can shift leadership away from the preferred replica.
2. **Preferred Replica Election**:
   - Use `kafka-leader-election.sh --election-type PREFERRED` (or enable `auto.leader.rebalance.enable=true`) to restore leadership to preferred replicas and keep the cluster balanced.
3. **ISR Election Mechanics**:
   - When the leader fails, the controller selects a new leader from the **In-Sync Replica (ISR)** set. Only replicas fully caught up to the leader's log end offset are eligible, guaranteeing no acknowledged data is lost.
4. **Unclean Leader Election**:
   - If the ISR is empty, Kafka must choose between availability and durability:
     - `unclean.leader.election.enable=true` -- an out-of-sync replica becomes leader, restoring availability but **risking message loss**.
     - `unclean.leader.election.enable=false` (default since 0.11) -- the partition stays offline until an ISR member recovers, preserving durability.

### Partition Reassignment

Partition reassignment redistributes replicas across brokers when adding new brokers, decommissioning old ones, or rebalancing after uneven growth.

1. **Generating a Reassignment Plan**:
   - Create a JSON file listing the topics to move (`topics.json`), then generate a plan:

```json
{"topics": [{"topic": "orders"}, {"topic": "payments"}], "version": 1}
```

```
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --topics-to-move-json-file topics.json --broker-list "1,2,3,4" --generate
```

2. **Reassignment JSON Format**:
   - The plan specifies the target replica list per partition:

```json
{"partitions": [
  {"topic": "orders", "partition": 0, "replicas": [2, 3]},
  {"topic": "orders", "partition": 1, "replicas": [3, 4]},
  {"topic": "orders", "partition": 2, "replicas": [4, 2]}
], "version": 1}
```

3. **Executing with Throttling**:
   - Use `--throttle` to cap inter-broker replication bandwidth:

```
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file reassignment.json --execute --throttle 50000000
```

4. **Verifying Completion**:

```
kafka-reassign-partitions.sh --bootstrap-server localhost:9092 \
  --reassignment-json-file reassignment.json --verify
```
   Output shows each partition's status: `completed successfully`, `still in progress`, or `failed`. Throttles are removed on completion.

### Common Pitfalls

1. **Hot Partitions**:
   - If message keys are not evenly distributed, some partitions may receive far more data than others. Monitor partition sizes and choose keys that distribute evenly.
2. **Too Few Partitions**:
   - Under-partitioned topics limit parallelism and create bottlenecks. The partition count cannot be decreased after creation; only increased.
3. **Too Many Partitions**:
   - Over-partitioning increases metadata overhead, memory usage, and leader election time. Aim for a balance between parallelism and resource consumption.
