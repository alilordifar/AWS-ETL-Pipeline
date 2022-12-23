from confluent_kafka import Producer
import json
from data import create_data
import time


if __name__ == '__main__':

    conf = {
        'bootstrap.servers': 'your bootstrap server',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'your username',
        'sasl.password': 'your password'
    }

    producer = Producer(conf)

    topic = 'Your Kafka Topic'

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
        producer.produce(topic, key=json_serializer(data['Specify Key']),
                         value=json_serializer(data), callback=acked)

        producer.poll(1)
        time.sleep(5)
