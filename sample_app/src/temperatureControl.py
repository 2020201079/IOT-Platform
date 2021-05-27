import sys
import platform_libfile
import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import KafkaConsumer
import os
import math

kafka_address = os.environ['KAFKA_ADDRESS']

def json_serializer(data):
    return data.encode()

producer = KafkaProducer(bootstrap_servers=[kafka_address],
                         value_serializer=json_serializer)


def getCoordinates(gps):
    _,bus_id,x,y = gps.split(":")
    return tuple([float(x),float(y)]),bus_id

def filled(bus_id):
    return True
    #should access data base to get count of people

def temperatureControl():
    #bus_temp_topicName = platform_libfile.getSensorData(sys.argv[1],0)
    bus_temp_topicName = 'bus_temp'

    #bus_gps_topicName = platform_libfile.getSensorData(sys.argv[2],0)
    bus_gps_topicName = 'bus_gps'

    #temp_Control_topic_name = platform_libfile.setSensorData(sys.argv[1],0)
    temp_Control_topic_name = 'temp_cont_bus1'

    consumer_bus_gps = KafkaConsumer(bus_gps_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_bus_temp = KafkaConsumer(bus_temp_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")

    for msg in consumer_bus_gps:
        _,bus_id = getCoordinates(msg.value.decode())
        break

    for msg_temp in consumer_bus_temp:
        if(filled(bus_id)):
            temp = float(msg_temp.value.decode('utf-8'))
            print(temp)
            if(temp > 20 ):
                print("switching on AC ")
                producer.send(temp_Control_topic_name, '1')
                dahsboardMsg = json.dumps({"Temperature": 'Switch on AC as temp is {}'.format(temp)})
                #print("topic name : ",'bus_'+bus_id)
                producer.send('bus_'+bus_id,dahsboardMsg) 


temperatureControl()