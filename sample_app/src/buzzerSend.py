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

def getCoordinates(gps):
    _,bus_id,x,y = gps.split(":")
    return tuple([float(x),float(y)]),bus_id

def getDistance(a,b):
    return math.sqrt((a[0]-b[0])**2 + (a[1] - b[1])**2)

def json_serializer(data):
    return data.encode()
producer = KafkaProducer(bootstrap_servers=[kafka_address],
                         value_serializer=json_serializer)

def buzzerSend():
    #bus_1_gps_topicName = platform_libfile.getSensorData(sys.argv[1],0)
    bus_1_gps_topicName = 'bus_gps_1'

    #bus_2_gps_topicName = platform_libfile.getSensorData(sys.argv[2],0)
    bus_2_gps_topicName = 'bus_gps_2'

    #bus_3_gps_topicName = platform_libfile.getSensorData(sys.argv[3],0)
    bus_3_gps_topicName = 'bus_gps_3'

    #bus_4_gps_topicName = platform_libfile.getSensorData(sys.argv[4],0)
    bus_4_gps_topicName = 'bus_gps_4'

    consumer_bus1_gps = KafkaConsumer(bus_1_gps_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_bus2_gps = KafkaConsumer(bus_2_gps_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_bus3_gps = KafkaConsumer(bus_3_gps_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_bus4_gps = KafkaConsumer(bus_4_gps_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")

    gps_coord = []
    ids = []

    for msg in consumer_bus1_gps:
        bus_1_gps,bus_1_id = getCoordinates(msg.value.decode())
        gps_coord.append(bus_1_gps)
        ids.append(bus_1_id)
        break

    for msg in consumer_bus2_gps:
        bus_2_gps,bus_2_id = getCoordinates(msg.value.decode())
        gps_coord.append(bus_2_gps)
        ids.append(bus_2_id)
        break

    for msg in consumer_bus3_gps:
        bus_3_gps,bus_3_id = getCoordinates(msg.value.decode())
        gps_coord.append(bus_3_gps)
        ids.append(bus_3_id)
        break

    for msg in consumer_bus4_gps:
        bus_4_gps,bus_4_id = getCoordinates(msg.value.decode())
        gps_coord.append(bus_4_gps)
        ids.append(bus_4_id)
        break

    radius = 60

    for i in range(0,len(gps_coord)):
        allDist = []
        for j in range(0,len(gps_coord)):
            if(i==j):
                continue
            currDist = getDistance(gps_coord[i],gps_coord[j])
            #print(i," ",j," ",currDist)
            if(currDist <= radius):
                allDist.append(ids[j])
            #print('len of list {}'.format(len(allDist)))
        if(len(allDist)>=3):
            for bus_id in allDist:
                dahsboardMsg = json.dumps({"Buzzer": 'BUZZER recieved{} busses are within {}'.format(str(allDist),radius)})
                print("topic name : ",'bus_'+bus_id)
                producer.send('bus_'+bus_id,dahsboardMsg) 
                print(dahsboardMsg)

buzzerSend()