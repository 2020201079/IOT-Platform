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
def create_lux():
    global set_lux
    if set_lux == 0:
        return int(random.uniform(20, 500))
    else:
        return set_lux


def get_data():

    return {
        "light": create_lux()
    }


producer = KafkaProducer(bootstrap_servers=[kafka_address],
                         value_serializer=json_serializer)
control_topic = sys.argv[2]

set_lux=0

def pause_data():
    global set_lux
    time.sleep(40)
    set_lux= 0


# control function
def set_data(data):
    global set_lux

    if (int(data) ==1):
        set_lux = 70
        threading.Thread(target=pause_data, args=()).start()
        print('Lights On')
    elif (int(data) == 0):
        set_lux = 28
        threading.Thread(target=pause_data, args=()).start()
        print('Lights Off')
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
    placeholder = sys.argv[3]

    while 1 == 1:
        registered_user = get_data()
        # print(registered_user["humidity"])
        #print("light")
        #print(str(registered_user["light"]))
        producer.send(topic_name, str(registered_user["light"]))
        time.sleep(5)
