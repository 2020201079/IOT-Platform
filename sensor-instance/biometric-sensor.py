import random
import sys
from faker import Faker
from kafka import KafkaProducer
import json
import time
import threading
from kafka import KafkaConsumer
import os

kafka_address = os.environ['KAFKA_ADDRESS']
def json_serializer(data):
    return data.encode()


def get_partition(key, all, available):
    return 0


#fake = Faker()
def create_biometric():
    return int(time.time())

def get_data():
    return {
        "biometric": create_biometric()
    }


producer = KafkaProducer(bootstrap_servers=[kafka_address],
                         value_serializer=json_serializer)
control_topic = sys.argv[2]


# control function to do
def set_data(data):
    if (data > 25):
        print('AC On')
    elif (data < 15):
        print('AC Off')
    else:
        print('Invalid Input')


def consumer_thread():
    consumer = KafkaConsumer(control_topic,
                             bootstrap_servers=kafka_address,
                             auto_offset_reset='earliest',
                             group_id='consumer-group-a')

    for msg in consumer:
        set_data(int(msg.value.decode()))


if __name__ == '__main__':
    topic_name = sys.argv[1]
    threading.Thread(target=consumer_thread, args=()).start()

    while 1 == 1:
        registered_user = get_data()
        # print(registered_user["humidity"])
        print("biometric")
        print(str(registered_user["biometric"]))
        producer.send(topic_name, str(registered_user["biometric"]))
        time.sleep(15)
