from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import json
import boto3


conf = {
    'bootstrap.servers': 'your bootstrap server',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'your username',
    'sasl.password': 'your password',
    'group.id': 'your group id',
    'auto.offset.reset': 'earliest'
}

# use boto3 client library to connect to the AWS Kinesis Data Stream
kinesis_client = boto3.client('kinesis',
                              region_name=AWS Region,
                              aws_access_key_id=AWS Access Key,
                              aws_secret_access_key=AWS Secret Access Key
                              )


consumer = Consumer(conf)
topic = ['List of Kafka topics']


def record_process(msg):
    '''
    This function is going to process
    messages on the Kafka Consumer Side
    '''
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
                # Uncomment below line  if you do not need to send data to KDS
                #msg = record_process(msg)
                # print(msg.value().decode('utf-8'))

                # Publishing records into AWS Kinesis Data Stream
                put_response = kinesis_client.put_record(
                    StreamName='demo',
                    Data=json.dumps(msg.value().decode('utf-8')),
                    PartitionKey='user_id')
                print(put_response)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


if __name__ == '__main__':
    consumer_loop(consumer, topic)
