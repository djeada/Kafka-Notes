## Case Study: Optimizing Kafka Producer Writes

Allegro, a major European e-commerce platform, achieved an **82% reduction in p99 tail latency** for Kafka producer writes by switching from ext4 to XFS. Their investigation, documented in the blog post "Unlocking Kafka's Potential by Tackling Tail Latency with eBPF", is a masterclass in using low-level tracing tools to diagnose performance problems that are invisible at the application layer.

### Overview

Allegro observed that while median Kafka produce latencies were acceptable, the tail latencies (p99 and above) were significantly elevated. Producers configured with `acks=all` were experiencing unpredictable latency spikes that impacted upstream services. Through a combination of Kafka protocol tracing and eBPF kernel instrumentation, they traced the root cause to ext4 file system journaling and lock contention during metadata commits. Switching the broker storage to XFS eliminated the contention and reduced p99 write latency by 82%.

### Background: How Kafka Writes Work at the OS Level

Kafka brokers store messages as append-only segment files on disk. On Linux, the write path relies heavily on the operating system's page cache rather than managing its own buffer pool. Here is the simplified write path:

```
+------------+      +------------+      +------------+      +-----------+      +--------+
|  Producer  | ---> |   Broker   | ---> | Page Cache | ---> |  Journal  | ---> |  Disk  |
| (send msg) |      | (write())  |      |  (memory)  |      | (metadata)|      |        |
+------------+      +------------+      +------------+      +-----------+      +--------+

      1. Producer sends a produce request to the broker.
      2. Broker appends data to the log segment via write() system call.
      3. Data lands in the OS page cache (returns immediately).
      4. The file system journals metadata changes (e.g., file size, timestamps).
      5. Dirty pages are flushed to disk asynchronously by pdflush/writeback.
```

Key points about this write path:

1. **write() is fast** — The broker's write system call copies data into the page cache and returns. It does not wait for data to hit disk.
2. **fsync is expensive** — If `log.flush.interval.messages` or `log.flush.interval.ms` is configured, Kafka calls fsync, which blocks until data is durable. Most deployments rely on replication instead of fsync.
3. **Metadata journaling still happens** — Even without explicit fsync calls, the file system must journal metadata changes (file size updates, inode modifications). This journaling can block write() calls if locks are held.

### Why Tail Latency Matters

Producers using `acks=all` must wait for the leader and all in-sync replicas to acknowledge the write. If any single broker in the replica set experiences a latency spike, the entire produce request is delayed. This makes tail latency (p99, p999) far more impactful than median latency — a single slow broker poisons the entire request.

### The Investigation

#### Step 1: Protocol-Level Tracing

Allegro began by instrumenting the Kafka protocol layer to measure how long individual produce requests took on each broker. They found that most requests completed in under 5ms, but a small percentage spiked to 30–100ms+ with no obvious application-level cause. The spikes were periodic and correlated across partitions on the same broker, suggesting an infrastructure-level bottleneck rather than a topic or partition issue.

#### Step 2: eBPF Kernel Tracing

To look below the application layer, Allegro deployed eBPF probes to trace system calls made by the Kafka broker process. They attached probes to:

- `write()` — to measure time spent in the write system call
- `fsync()` / `fdatasync()` — to measure explicit flush operations
- `pwrite64()` — to trace positional writes to log segments

The eBPF traces revealed that the `write()` system call itself was occasionally blocking for tens of milliseconds, even though write() should only copy data into the page cache. This pointed directly at the file system layer.

#### Step 3: Identifying the Bottleneck

Further tracing with eBPF probes on file system internals revealed the culprit: **ext4 journaling lock contention**. The JBD2 (Journaling Block Device 2) thread was periodically acquiring exclusive locks during journal commits, and concurrent write() calls from the Kafka broker were blocked waiting for these locks.

### ext4 Analysis

ext4 uses a journal to protect file system metadata integrity. Understanding how this journal works explains why it causes latency spikes for write-heavy workloads like Kafka.

#### How ext4 Journaling Works

ext4 supports three journaling modes:

| Mode | What is Journaled | Performance | Safety |
|---|---|---|---|
| **journal** | Data + metadata | Slowest | Highest |
| **ordered** (default) | Metadata only, data written before metadata commit | Medium | High |
| **writeback** | Metadata only, no data ordering | Fastest | Lower |

In the default **ordered** mode:

1. Data is written to the page cache via write().
2. The JBD2 thread wakes up every **commit interval** (default: 5 seconds) to commit journal transactions.
3. Before committing metadata, JBD2 forces all dirty data pages for affected files to be flushed to disk (to maintain ordering guarantees).
4. During the commit, JBD2 holds a **transaction lock** that blocks new metadata-modifying operations.
5. Any write() that extends a file (which modifies metadata like file size) must wait for the lock.

#### Why This Causes Latency Spikes

Kafka continuously appends to log segment files, meaning every write extends the file and modifies metadata. When the JBD2 commit runs:

```
Timeline:
                    JBD2 commit (holds lock)
                    |<--- 10-50ms+ --->|
                    v                  v
Kafka write():  OK OK OK [BLOCKED........] OK OK OK
                              ^
                              |
                     write() waits for journal
                     transaction lock release
```

The default 5-second commit interval means these spikes happen periodically. Under heavy write load, the journal commit can take 10–50ms or more, directly impacting any concurrent write() calls.

### Tuning Attempts on ext4

Allegro systematically tried several ext4 tuning approaches before concluding that the file system itself needed to change.

#### 1. Reducing the Commit Interval

```bash
# Default is 5 seconds; reduced to 1 second
mount -o commit=1 /dev/sdX /kafka-data
```

**Result:** More frequent commits meant each individual commit was smaller and faster, reducing the duration of lock contention. However, the spikes became more frequent even if shorter. Net improvement was modest.

#### 2. Switching to Writeback Journal Mode

```bash
# Removes data ordering guarantee
tune2fs -o journal_data_writeback /dev/sdX
mount -o data=writeback /dev/sdX /kafka-data
```

**Result:** Eliminated the need to flush data pages before metadata commits, reducing commit duration. Improved tail latency but introduced risk — a crash could leave data and metadata inconsistent. For Kafka with replication, this risk is generally acceptable since data can be recovered from replicas.

#### 3. Enabling fast_commit

ext4's `fast_commit` feature (available in newer kernels) reduces journal commit overhead by logging fine-grained changes instead of full metadata blocks.

```bash
tune2fs -O fast_commit /dev/sdX
```

**Result:** Reduced commit latency further, but the fundamental lock contention issue remained. Improvements were incremental rather than transformative.

#### Summary of ext4 Tuning Results

| Tuning | p99 Improvement | Trade-off |
|---|---|---|
| Reduced commit interval | ~15-20% | More frequent (shorter) spikes |
| Writeback mode | ~30-40% | Reduced crash consistency |
| fast_commit | ~10-15% additional | Requires newer kernel |
| All combined | ~50% | Still had periodic spikes |

Even with all tunings combined, the fundamental architecture of ext4 journaling — a single global journal with transaction-level locking — remained a bottleneck for Kafka's write pattern.

### Switching to XFS

XFS was designed from the ground up for high-performance parallel I/O on large storage systems. Several architectural differences make it fundamentally better suited for Kafka's workload.

#### Why XFS Handles Metadata Differently

1. **Allocation Groups:** XFS divides the file system into independent allocation groups (AGs), each with its own metadata structures and locks. Writes to files in different AGs do not contend with each other. In contrast, ext4 uses a single global journal.

```
ext4:                                XFS:
+---------------------------+        +--------+ +--------+ +--------+
|     Single Global Journal |        |  AG 0  | |  AG 1  | |  AG 2  |
|  (one lock for all files) |        | (lock) | | (lock) | | (lock) |
+---------------------------+        +--------+ +--------+ +--------+
All writes compete for       -->     Writes distributed across
the same journal lock                independent allocation groups
```

2. **Delayed Allocation:** XFS aggressively delays block allocation decisions until data is actually flushed, reducing the number of metadata transactions needed during the write path.

3. **Pre-allocation Strategy:** XFS uses speculative pre-allocation — when a file is being extended by sequential writes, XFS pre-allocates larger extents in anticipation of future writes. This means fewer metadata updates per write compared to ext4's more conservative allocation.

4. **Log Design:** The XFS log (equivalent to ext4's journal) uses a different locking strategy that allows concurrent transactions and does not require holding locks for the entire commit duration.

#### Results After Switching to XFS

The migration from ext4 to XFS on the same hardware produced dramatic improvements:

| Metric | ext4 (tuned) | XFS | Improvement |
|---|---|---|---|
| p50 latency | ~2ms | ~1.5ms | 25% |
| p99 latency | ~45ms | ~8ms | **82%** |
| p999 latency | ~120ms | ~15ms | 87% |
| Latency variance | High | Low | Significant |

The periodic latency spikes that characterized ext4 behavior were virtually eliminated. The p99 latency became consistent and predictable, which is exactly what upstream services need for reliable SLA management.

### Key Takeaways

- **Tail latency matters more than throughput** for Kafka producers using `acks=all`. A single slow write on any replica delays the entire produce request.
- **The write() system call is not always fast.** File system journaling can cause write() to block even when data is only going to the page cache.
- **eBPF is invaluable for diagnosing kernel-level performance issues** that are invisible to application-level monitoring and profiling.
- **ext4's single global journal is a structural bottleneck** for workloads with many concurrent file extensions across multiple files (like Kafka's log segments).
- **XFS allocation groups provide natural parallelism** that matches Kafka's multi-partition, multi-segment write pattern.
- **File system choice is a first-order performance decision** for Kafka brokers — not just a deployment detail.
- **Always look at tail latencies (p99, p999)**, not just averages. The journaling issue was invisible in median latency metrics.

### Configuration Recommendations

| Setting | ext4 | XFS | Notes |
|---|---|---|---|
| **File system** | ext4 | **XFS (recommended)** | XFS eliminates journal lock contention |
| **Journal mode** | `data=writeback` | N/A | Only if staying on ext4 |
| **Commit interval** | `commit=1` | N/A | Reduces spike duration on ext4 |
| **Mount options** | `noatime,nodiratime` | `noatime,nodiratime` | Reduces unnecessary metadata writes |
| **fast_commit** | Enable if kernel supports | N/A | Incremental improvement on ext4 |
| **Scheduler** | `none` (for NVMe) / `deadline` | `none` (for NVMe) / `deadline` | Match scheduler to storage type |
| **Kafka log.flush** | Rely on replication | Rely on replication | Avoid explicit fsync for throughput |
| **Kafka log.dirs** | Spread across disks | Spread across disks | Maximizes AG parallelism on XFS |
