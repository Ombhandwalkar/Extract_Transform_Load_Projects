from kafka import KafkaConsumer
from time import sleep
from json import dumps,loads
import json
from s3fs import S3FileSystem



KAFKA_TOPIC='stock_market' # Topic name
KAFKA_SERVER="13.51.201.186:9092"   # Public ec2 instance ip

def receive_data():
    consumer=KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_server=KAFKA_SERVER,
        value_deserialize= lambda x: loads(x.decode('utf-8'))

    )
    s3=S3FileSystem()
    for count, i in enumerate(consumer):
        with s3.open("s3://kafka-stock-market-demo/stock_market_{}.json".format(count), 'w') as file:
            json.dump(i.value, file) 