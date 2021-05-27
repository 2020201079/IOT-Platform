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

bus_cordinates = {"1":50, "2":0, "3":-70, "4":0}



#fake = Faker()
def create_x_y(type,id):
    if id =="1":
        bus_cordinates[id]=bus_cordinates[id]- 2.6
        if bus_cordinates[id] <= 0:
            bus_cordinates[id] = 50

    if id =="2":
        bus_cordinates[id]=bus_cordinates[id]+2.6
        if bus_cordinates[id] >=50 :
            bus_cordinates[id] = 0

    if id =="3":
        bus_cordinates[id]=bus_cordinates[id]+2.6
        if bus_cordinates[id] >= 0 :
            bus_cordinates[id] = -70

    if id =="4":
        bus_cordinates[id]=bus_cordinates[id]+2.6
        if bus_cordinates[id] >= -70 :
            bus_cordinates[id] = 0

    return str(bus_cordinates[id])+ ":" +"0"
    #return str(random.uniform(0,50)) + ":" + str(random.uniform(0,50))


cordinates= {"1":"1:0", "2":"-1:0","3":"0:1","4":"0:-1"}



def get_data(placeholder):


    type,id=placeholder.split(":")
    if type == "admin":
        return {
            "xy": placeholder+":"+"0:0"
        }
    elif type == "barricade":
        return {
            "xy": placeholder +":" + cordinates[id]
        }
    else:
        return {
            "xy": placeholder+":"+create_x_y(type,id)
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
    placeholder = sys.argv[3]
    while 1 == 1:
        registered_user = get_data(placeholder)
        # print(registered_user["humidity"])
        a,b,_,_=registered_user["xy"].split(":")
        print(a + b +" " + str(registered_user["xy"]))
        producer.send(topic_name, str(registered_user["xy"]))
        #data generated string- example- "bus:1:54:0" --- bus:id:x:y
        time.sleep(5)
