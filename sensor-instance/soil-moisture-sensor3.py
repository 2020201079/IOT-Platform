import sys
from faker import Faker
from kafka import KafkaProducer
import json
import time
import os
from kafka import KafkaConsumer

def json_serializer(data):
    return data.encode()

def get_partition(key,all,available):
    return 0

fake = Faker()

def get_data():
    print("i am called")
    return {
        "humidity": fake.pydecimal()
    }

kafka_address = os.environ['KAFKA_ADDRESS']


producer = KafkaProducer(bootstrap_servers=[kafka_address],
                         value_serializer=json_serializer)



if __name__=='__main__':
    topic_name = sys.argv[1]
    while 1 == 1:
        registered_user = get_data()
        #print(registered_user["humidity"])
        producer.send(topic_name,str(registered_user["humidity"]))
        time.sleep(5)
