
- How Kafka Works?
- Why Producers Writes are Slow
- How Allegro Traced the Kafka Protocol 
- How Tracing Kernel System Calls
- What is a Journaled File Systems
- How Allegro Tuned ext4
- And finally Switching to XFS

On Linux, Kafka uses regular files for storing data. Writes are done using ordinary write system calls — data is first stored in the page cache and then asynchronously flushed to disk"
"Let’s go back to ext4. We knew that journal commits were the source of latency". Hence the choice for XFS which seemingly handles the process better.

x amount of data copied from a socket to userspace memory, then from userspace to page cache memory, then page cache to disk vs direct socket to disk is the true power of zero copy (kafka uses it).

Another big problem with ext4 is its slow and non-tunable allocation algorithm. Prefer burst of large pre-allocation then write data, it will be way more faster.

If kernel TLS is enabled then SSL_sendfile() can be used (userspace encryption may be performant but has a higher chance of not using the right cpu instructions to accelerate the decryption). I hardly doubt though that somebody made the patch for SSL_sendfile() in kafka. Nginx uses it like a pro. In small organisation kafka is exposed inside a secure network and tls is disabled for performance. TBH kafka is not supposed to be external client facing. Also zero security is not practiced inside small organisations. 

