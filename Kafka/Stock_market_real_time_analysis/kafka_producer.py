from kafka import KafkaProducer
from json import dumps
from time import sleep
import pandas as pd


KAFKA_TOPIC='stock_market' # Topic name
KAFKA_SERVER="13.51.201.186:9092"   # Public ec2 instance ip
df= pd.read_csv('indexProcessed.csv') # Data 

def send_data():
     # Create the Producer to produce data 
    producer= KafkaProducer(
        bootstrap_server=KAFKA_SERVER,
        value_serializer=lambda x:dumps(x).encoder('utf-8') # Encrypt data inot Binary format
    )
    while True:
        dict_stock= df.sample(0).to_dict(orient='records')[0]
        producer.send(KAFKA_TOPIC,value=dict_stock)
        sleep(2)

    
if __name__=="__main__":
    send_data()