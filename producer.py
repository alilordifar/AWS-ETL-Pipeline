from confluent_kafka import Producer
import json
from data import create_data
import time


if __name__ == '__main__':

    conf = {
        'bootstrap.servers': 'pkc-n00kk.us-east-1.aws.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'KJZAKGCEEXUVNFHV',
        'sasl.password': '/K5XNN7fTWl7h3lqN79NJA7ox+92g+KfVXAHa+4Gm3WI7Cmky1REPoq1Wb2amjoW',
    }

    producer = Producer(conf)

    topic = 'twitter-topic'

    def json_serializer(data):
        return json.dumps(data)

    def acked(err, msg):
        if err is not None:
            print(f'Failed to deliver message: {str(msg)}, {str(err)}')
        else:
            print(f'Message produced succesfully: {str(msg)}')

    while True:
        data = create_data()
        print(data)
        producer.produce(topic, json_serializer(data), callback=acked)
        producer.poll(1)
        time.sleep(5)
