## Task 10: Tuning, Performance, and Reliability

**Objectives:**
- Optimize Kafka broker and client configurations for high throughput or low latency.
- Adjust producer settings (batch size, linger.ms), consumer settings (fetch.min.bytes).
- Discuss in-sync replicas (ISR) and leader election for reliability.

**Lab Steps:**

1. **Producer Tuning:**  
   - Experiment with settings in Python:
     ```python
     producer = KafkaProducer(
         bootstrap_servers='localhost:9092',
         linger_ms=5,
         batch_size=32768,
         compression_type='snappy'
     )
     ```
   - Measure throughput by sending large volumes of messages and time them.

2. **Broker Tuning:**  
   - Adjust `num.network.threads`, `num.io.threads`, or `socket.send.buffer.bytes` in the broker config.  
   - Evaluate disk I/O and memory usage.

3. **Compression and Acks:**  
   - Use producer config `acks='all'` for stronger durability at the cost of higher latency.  
   - Compare `acks=1` or `acks=0` for speed.

4. **ISR and Min In-Sync Replicas (If Multi-Broker):**  
   - Set `min.insync.replicas=2` in your topic config or broker config for higher reliability.  
   - Monitor the effect on throughput.

5. **Reflection:**  
   Document the trade-offs between throughput, latency, and reliability. Note how real production tuning often requires iterative testing under realistic loads.
