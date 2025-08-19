from kafka import KafkaProducer
import os 
import time

KAFKA_TOPIC='demo-topic'
KAFKA_SERVER= 'localhost:9092'
EMAIL_FILE= 'email.txt'

def send_to_kafka():
    producer= KafkaProducer(bootstrap_servers=KAFKA_SERVER)
    seen_email=set()

    while True:
        if os.path.exists(EMAIL_FILE):
            with open(EMAIL_FILE,'r',encoding='utf-8')as f: # Encoding the data into binary format.
                for email in f:
                    email =email.strip()
                    # Check for duplicate email,if not then send it to the "demo-topic"
                    if email and email not in seen_email:
                        producer.send(KAFKA_TOPIC, email.encode('utf-8'))
        time.sleep(2)
    producer.flush()
    producer.close()

if __name__=='__main__':
    send_to_kafka()