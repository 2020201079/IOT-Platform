import sys
import platform_libfile
import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import KafkaConsumer
import os
import math
from pymongo import MongoClient

kafka_address = os.environ['KAFKA_ADDRESS']

cluster = MongoClient("mongodb+srv://shweta_10:shweta10@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = cluster["session_storage"]
collection = db["bus_PassengerDetails"]

def json_serializer(data):
    return data.encode()
producer = KafkaProducer(bootstrap_servers=[kafka_address],
                         value_serializer=json_serializer)

def getCoordinates(gps):
    _,bus_id,x,y = gps.split(":")
    return tuple([float(x),float(y)]),bus_id

def getDistance(a,b):
    return math.sqrt((a[0]-b[0])**2 + (a[1] - b[1])**2)

def baricadeDetector():

    #bus_gps_topicName = platform_libfile.getSensorData(sys.argv[1],0)
    bus_gps_topicName = 'bus_gps_1'

    #barricade_gps1_topicName = platform_libfile.getSensorData(sys.argv[2],0)
    barricade_gps1_topicName = 'barricade_gps_1'

    #barricade_gps2_topicName = platform_libfile.getSensorData(sys.argv[3],0)
    barricade_gps2_topicName = 'barricade_gps_2'

    #barricade_gps3_topicName = platform_libfile.getSensorData(sys.argv[4],0)
    barricade_gps3_topicName = 'barricade_gps_3'

    #barricade_gps4_topicName = platform_libfile.getSensorData(sys.argv[5],0)
    barricade_gps4_topicName = 'barricade_gps_4'

    consumer_bus_gps = KafkaConsumer(bus_gps_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_barricade1_gps = KafkaConsumer(barricade_gps1_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_barricade2_gps = KafkaConsumer(barricade_gps2_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_barricade3_gps = KafkaConsumer(barricade_gps3_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")
    consumer_barricade4_gps = KafkaConsumer(barricade_gps4_topicName,bootstrap_servers=kafka_address,auto_offset_reset = "latest")

    barr_coord = []
    barr1_coor = None
    for msg_barricade_1 in consumer_barricade1_gps:
        barr1_coor = msg_barricade_1.value.decode('utf-8')
        break
    barr1_coor,_ = getCoordinates(barr1_coor)
    barr_coord.append(barr1_coor)

    barr2_coor = None
    for msg_barricade_1 in consumer_barricade2_gps:
        barr2_coor = msg_barricade_1.value.decode('utf-8')
        break
    barr2_coor,_ = getCoordinates(barr2_coor)
    barr_coord.append(barr2_coor)

    barr3_coor = None
    for msg_barricade_1 in consumer_barricade3_gps:
        barr3_coor = msg_barricade_1.value.decode('utf-8')
        break
    barr3_coor,_ = getCoordinates(barr3_coor)
    barr_coord.append(barr3_coor)

    barr4_coor = None
    for msg_barricade_1 in consumer_barricade4_gps:
        barr4_coor = msg_barricade_1.value.decode('utf-8')
        break
    barr4_coor,_ = getCoordinates(barr4_coor)
    barr_coord.append(barr4_coor)

    threshold = 3
    for msg in consumer_bus_gps:
        bus_coord,bus_id = getCoordinates(msg.value.decode('utf-8'))
        print("bus gps ",bus_coord)
        for c in barr_coord:
            if(getDistance(c,bus_coord) < threshold):
                collection.update_one({'bus_id':bus_id},{"$set": {'bus_id':bus_id,'is_filled': False}}, upsert=True)
                print("bus has reached barricade ",c)
                dahsboardMsg = json.dumps({"Barricade": 'bus has reached barricade {}'.format(c)})
                producer.send('bus_'+bus_id,dahsboardMsg) 
        
baricadeDetector()