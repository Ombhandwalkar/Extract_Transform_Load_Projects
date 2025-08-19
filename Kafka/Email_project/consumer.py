from kafka import KafkaConsumer 
import os 
import time

KAFKA_TOPIC= 'demo-topic'
KAFKA_SERVER= 'localhost:9092'


def consume_from_kafka():
    consumer= KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers= KAFKA_SERVER,
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    print('Watiting for the message...')
    for message in consumer:
        email= message.value.decode('utf-8')
        print(f'New signup email: {email}')


if __name__=="__main__":
    consume_from_kafka()