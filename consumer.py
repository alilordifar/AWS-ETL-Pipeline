from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

conf = {
    'bootstrap.servers': 'pkc-n00kk.us-east-1.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'KJZAKGCEEXUVNFHV',
    'sasl.password': '/K5XNN7fTWl7h3lqN79NJA7ox+92g+KfVXAHa+4Gm3WI7Cmky1REPoq1Wb2amjoW',
    'group.id': 'test',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
topic = ['twitter-topic']


def record_process(msg):
    print(
        f"Recieved Records: {msg.value().decode('utf-8')}")


def consumer_loop(consumer, topics):
    try:
        consumer.subscribe(['twitter-topic'])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' % (
                        msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                record_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


if __name__ == '__main__':
    consumer_loop(consumer, topic)
