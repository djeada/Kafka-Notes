from kafka import KafkaConsumer, KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
consumer = KafkaConsumer('my-topic', bootstrap_servers='localhost:9092', group_id='my-group')

for message in consumer:
    # Process message
    print(f"Consumed: {message.value.decode('utf-8')}")
    
    # Send acknowledgment
    producer.send('ack-topic', f"Ack for message {message.value.decode('utf-8')}")

    # using offset and partition
    # ack_message = f"Processed message from partition {message.partition} with offset {message.offset}."
    # producer.send('ack-topic', ack_message)

# On the producer side, consume from 'ack-topic' to get acknowledgments
ack_consumer = KafkaConsumer('ack-topic', bootstrap_servers='localhost:9092')
for ack in ack_consumer:
    print(f"Received ack: {ack.value.decode('utf-8')}")
