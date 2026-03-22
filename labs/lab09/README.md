## Task 9: Kafka Security (SSL, SASL, ACLs)

**Objectives:**
- Configure SSL encryption for client-broker communication.
- Enable SASL/PLAIN or SASL/SCRAM authentication for Kafka clients.
- Use Kafka ACLs to authorize operations (topic read, write, etc.).

> **Note:** Security steps can be intricate—this lab covers fundamentals.

**Lab Steps:**

1. **Generate SSL Certificates (on your host or a separate container):**
   ```bash
   # Example using openssl
   openssl req -newkey rsa:2048 -nodes -keyout kafka.key -x509 -days 365 -out kafka.crt -subj "/CN=localhost"
   ```
   - Copy certificates to the Kafka container or mount them via volumes.

2. **Configure SSL in Kafka:**  
   In `docker-compose.yml`, set environment variables for the Kafka broker, e.g.:
   ```yaml
   environment:
     KAFKA_LISTENERS: SSL://0.0.0.0:9093,PLAINTEXT://0.0.0.0:9092
     KAFKA_ADVERTISED_LISTENERS: SSL://localhost:9093,PLAINTEXT://localhost:9092
     KAFKA_SSL_KEYSTORE_FILENAME: "kafka.keystore.jks"
     KAFKA_SSL_KEYSTORE_CREDENTIALS: "keystore_creds"
     KAFKA_SSL_KEY_PASSWORD: "keystore_pass"
     # ...
   ```
   (Exact steps vary—this is just a schematic.)

3. **Client Configuration (Python SSL):**
   ```python
   from kafka import KafkaProducer
   producer = KafkaProducer(
       bootstrap_servers='localhost:9093',
       security_protocol='SSL',
       ssl_cafile='ca.crt',
       ssl_certfile='client.crt',
       ssl_keyfile='client.key'
   )
   ```
   - Produce or consume messages to test connectivity.

4. **SASL and ACLs (Optional Advanced):**  
   - Enable SASL in the broker config.  
   - Create user credentials.  
   - Apply ACLs to allow or deny user operations:
     ```bash
     kafka-acls --authorizer-properties zookeeper.connect=zookeeper:2181 \
       --add --allow-principal User:myuser --operation Read --topic secure_topic
     ```

5. **Reflection:**  
   Summarize how security measures protect data in transit and ensure only authorized clients can produce/consume. Note the complexity of managing certificates and ACL rules.
