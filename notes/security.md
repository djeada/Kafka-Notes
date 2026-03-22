## Kafka Security

Kafka security encompasses three core pillars: encryption, authentication, and authorization. Together they ensure that data in the cluster is protected from eavesdropping, only verified clients can connect, and each principal can only perform operations it has been explicitly granted.

### Security Layers Overview

```
+---------------------+
|      Client         |
| (Producer/Consumer) |
+----------+----------+
           |
           | 1. Authentication (SASL / mTLS)
           v
+----------+----------+
|   Identity Verified  |
+----------+----------+
           |
           | 2. Authorization (ACLs)
           v
+----------+----------+
|  Permission Granted  |
+----------+----------+
           |
           | 3. Encryption (SSL/TLS)
           v
+----------+----------+
|     Kafka Broker     |
+----------------------+
```

```
Client ---SSL/TLS---> Broker 1 ---SSL/TLS---> Broker 2
  |                      |                       |
  +-- SASL Auth -+       +-- Inter-broker TLS ---+
                 v
        +----------------+
        | ACL Check      |
        | Principal: User|
        | Resource: Topic|
        | Operation: READ|
        +----------------+
```

---

### Encryption with SSL/TLS

Kafka supports SSL/TLS for encrypting data in transit between clients and brokers and between brokers themselves. This prevents eavesdropping and man-in-the-middle attacks.

#### Generating Keystores and Truststores

```
# 1. Generate a CA key and certificate
openssl req -new -x509 -keyout ca-key -out ca-cert -days 365 \
  -subj "/CN=KafkaCA" -passout pass:ca-secret

# 2. Generate broker keystore
keytool -keystore kafka.server.keystore.jks -alias broker \
  -validity 365 -genkey -keyalg RSA -storepass broker-secret \
  -dname "CN=broker1.example.com"

# 3. Create CSR, sign with CA, import back into keystore
keytool -keystore kafka.server.keystore.jks -alias broker \
  -certreq -file broker-csr -storepass broker-secret
openssl x509 -req -CA ca-cert -CAkey ca-key -in broker-csr \
  -out broker-cert-signed -days 365 -CAcreateserial -passin pass:ca-secret
keytool -keystore kafka.server.keystore.jks -alias CARoot \
  -import -file ca-cert -storepass broker-secret -noprompt
keytool -keystore kafka.server.keystore.jks -alias broker \
  -import -file broker-cert-signed -storepass broker-secret

# 4. Create truststore with CA certificate
keytool -keystore kafka.server.truststore.jks -alias CARoot \
  -import -file ca-cert -storepass truststore-secret -noprompt
```

#### Broker SSL Configuration (server.properties)

```
listeners=SSL://broker1.example.com:9093
security.inter.broker.protocol=SSL
ssl.keystore.location=/var/kafka/ssl/kafka.server.keystore.jks
ssl.keystore.password=broker-secret
ssl.key.password=broker-secret
ssl.truststore.location=/var/kafka/ssl/kafka.server.truststore.jks
ssl.truststore.password=truststore-secret
ssl.client.auth=required
ssl.enabled.protocols=TLSv1.2,TLSv1.3
```

* **`ssl.keystore.location`**: JKS keystore containing the broker's private key and certificate.
* **`ssl.truststore.location`**: JKS truststore containing trusted CA certificates.
* **`ssl.client.auth`**: Client cert authentication — `required`, `requested`, or `none`.

---

### Authentication

Kafka supports multiple SASL mechanisms. Authentication can be combined with SSL/TLS using the `SASL_SSL` security protocol.

#### SASL Mechanisms Comparison

```
+----------------+----------------+--------------+-----------------------------+
| Mechanism      | Security Level | Complexity   | Use Case                    |
+----------------+----------------+--------------+-----------------------------+
| PLAIN          | Low            | Very Low     | Development / internal nets |
| SCRAM-SHA-256  | Medium         | Low          | Production without Kerberos |
| SCRAM-SHA-512  | High           | Low          | Production without Kerberos |
| GSSAPI         | Very High      | High         | Enterprise with Kerberos    |
| OAUTHBEARER    | High           | Medium       | Cloud-native / token-based  |
+----------------+----------------+--------------+-----------------------------+
```

#### Listener Configuration

```
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093
listener.security.protocol.map=INTERNAL:SASL_PLAINTEXT,EXTERNAL:SASL_SSL
security.inter.broker.protocol=SASL_PLAINTEXT
inter.broker.listener.name=INTERNAL
```

#### SASL/PLAIN Example

Broker JAAS file (`kafka_server_jaas.conf`):

```
KafkaServer {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="admin"
    password="admin-secret"
    user_admin="admin-secret"
    user_producer1="producer1-secret";
};
```

Client configuration:

```
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
  username="producer1" password="producer1-secret";
```

#### SASL/SCRAM Example

```
# Create SCRAM credentials in ZooKeeper
kafka-configs.sh --zookeeper localhost:2181 --alter \
  --add-config 'SCRAM-SHA-512=[password=alice-secret]' \
  --entity-type users --entity-name alice
```

Client configuration:

```
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="alice" password="alice-secret";
```

---

### Authorization with ACLs

Kafka ACLs govern which principals can perform which operations on which resources. They are evaluated by the `AclAuthorizer` (ZooKeeper) or `StandardAuthorizer` (KRaft).

```
+---------------------------------------------+
|               Kafka ACL Entry                |
+---------------------------------------------+
| Principal   :  User:alice                    |
| Resource    :  Topic:orders                  |
| Operation   :  READ                          |
| Permission  :  ALLOW                         |
| Host        :  *                             |
+---------------------------------------------+
```

* **Resource Types**: `Topic`, `Group`, `Cluster`, `TransactionalId`, `DelegationToken`
* **Operations**: `Read`, `Write`, `Create`, `Delete`, `Alter`, `Describe`, `All`
* **Permission Types**: `Allow` or `Deny` (Deny takes precedence)

#### Enabling ACLs

```
authorizer.class.name=kafka.security.authorizer.AclAuthorizer
super.users=User:admin;User:kafka
allow.everyone.if.no.acl.found=false
```

#### Managing ACLs with kafka-acls.sh

```
# Grant produce permission
kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:2181 \
  --add --allow-principal User:producer1 --operation Write --topic orders

# Grant consume permission (topic + consumer group)
kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:2181 \
  --add --allow-principal User:consumer1 --operation Read --topic orders
kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:2181 \
  --add --allow-principal User:consumer1 --operation Read --group order-processors

# Wildcard permissions (all topics)
kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:2181 \
  --add --allow-principal User:admin --operation All --topic '*'

# List all ACLs
kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:2181 --list

# Remove an ACL
kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:2181 \
  --remove --allow-principal User:producer1 --operation Write --topic orders
```

---

### Encryption at Rest

Kafka does **not** natively encrypt data at rest. Messages are stored as plaintext in log segment files. Use infrastructure-level encryption instead:

* **Linux dm-crypt / LUKS**: Encrypt the block device where Kafka log directories reside.
* **Cloud Provider KMS**: AWS EBS encryption, GCP Persistent Disk encryption, or Azure Disk Encryption.
* **Application-Level Encryption**: Encrypt payloads before producing. Provides end-to-end encryption but prevents broker-side compaction from inspecting keys.

---

### Multi-Tenancy Security

In shared clusters, use ACLs, naming conventions, and quotas to isolate tenants.

#### Topic Naming and Prefixed ACLs

```
# Convention: <tenant>.<domain>.<entity>
# Grant teamA access only to teamA-prefixed topics
kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:2181 \
  --add --allow-principal User:teamA-producer \
  --operation Write --topic teamA. --resource-pattern-type prefixed

kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:2181 \
  --add --allow-principal User:teamA-consumer \
  --operation Read --topic teamA. --resource-pattern-type prefixed
```

#### Quotas for Resource Isolation

```
# Limit producer throughput to 10 MB/s
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --add-config 'producer_byte_rate=10485760' \
  --entity-type users --entity-name teamA-producer

# Limit consumer throughput to 20 MB/s
kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --add-config 'consumer_byte_rate=20971520' \
  --entity-type users --entity-name teamA-consumer
```

---

### Common Pitfalls

1. **Using SASL/PLAIN without SSL/TLS**: PLAIN sends credentials in clear text. Always pair with `SASL_SSL`.
2. **`allow.everyone.if.no.acl.found=true` in production**: Grants full access to any resource without explicit ACLs. Always set to `false`.
3. **Forgetting consumer group ACLs**: `Read` on a topic alone is not enough — also grant `Read` on the consumer group resource.
4. **Expired or mismatched certificates**: Broker cert CN/SAN must match the advertised hostname. Automate renewal and monitor expiry.
5. **Passwords in plaintext config files**: Protect config files with `chmod 600` and consider a secrets manager.
6. **Unencrypted inter-broker traffic**: Set `security.inter.broker.protocol=SSL` or `SASL_SSL` to encrypt replication traffic.
7. **Overly broad wildcard ACLs**: Granting `All` on `Topic:*` to app users defeats authorization. Follow least privilege.

---

### Best Practices

1. **Enable TLS everywhere**: Use `SASL_SSL` for all listeners including inter-broker. Prefer TLSv1.3 for better security and performance.
2. **Use SCRAM-SHA-512 or GSSAPI in production**: Avoid PLAIN outside of development. SCRAM needs no external infrastructure; GSSAPI integrates with Kerberos.
3. **Principle of least privilege**: Producers only need `Write` + `Describe`; consumers need `Read` on topics and groups. Never grant more.
4. **Automate certificate rotation**: Use Vault, cert-manager, or similar. Kafka supports dynamic SSL keystore reload without restart.
5. **Audit ACLs regularly**: Review with `kafka-acls.sh --list` periodically. Remove stale entries for decommissioned apps.
6. **Use prefixed ACLs for multi-tenancy**: `--resource-pattern-type prefixed` scales better and enforces naming conventions.
7. **Secure ZooKeeper**: Enable SASL and restrict network access. ZooKeeper stores SCRAM credentials and ACL data.
8. **Monitor auth failures**: Track `FailedAuthenticationRate` metrics and ACL denial logs. Alert on unusual spikes.
