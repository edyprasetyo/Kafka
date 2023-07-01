from confluent_kafka import Consumer, KafkaError

print("Hello, World!")

conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['my-topic'])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition event')
        else:
            print('Error occured: {}'.format(msg.error().str()))
    else:
        print('Received message at {}: {}'.format(msg.topic(), msg.value().decode('utf-8')))