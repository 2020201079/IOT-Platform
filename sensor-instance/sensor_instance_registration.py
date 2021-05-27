from kafka import KafkaConsumer
import json
import time
from pymongo import MongoClient
from kafka.admin import KafkaAdminClient, NewTopic
import os
import threading
from datetime import datetime

kafka_address = os.environ['KAFKA_ADDRESS']
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_address,
    client_id='test'
)

cluster = MongoClient("mongodb+srv://shweta_10:shweta10@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = cluster["sensor_registory"]
collection = db["sensor_instance"]


def connect_sensor(name,topic_w,topic_control,placeholder):
    os.system('python3 ' + name + ' ' + topic_w+' '+topic_control + ' '+placeholder)


if __name__=='__main__':
    topic_name = "pm_to_sensor_ins_reg"
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_address,
        auto_offset_reset='earliest',
        group_id='consumer-group-c')

    print('starting the consumer')
    for msg in consumer:
        new_type=json.loads(msg.value)
        print(type(new_type))
        k=new_type["list_of_sensor_instances"]
        for x in k:
            stop_treads=False
            t = time.time()
            topic_w = "topic_in"+str(t)
            #topic_control = "topic_control"+str(t)
            topic_control = "topic_control_" + x["sensor_type"]+"_"+str(t)
            name= x["sensor_type"]+".py"
            placeholder="0"
            if x["sensor_type"]== "gps-sensor":
                if x["placeholder"] == "admin":
                    placeholder="admin"+":"+x["admin-id"]
                if x["placeholder"] == "barricade":
                    placeholder = "barricade" + ":" + x["barricade-id"]
                if x["placeholder"] == "bus":
                    placeholder = "bus" + ":" + x["bus-id"]



            topic_list = []
            topic_list.append(NewTopic(name=topic_w, num_partitions=1, replication_factor=1))
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            x["topic"]=topic_w

            topic_list = []
            topic_list.append(NewTopic(name=topic_control, num_partitions=1, replication_factor=1))
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            x["topic_control"] = topic_control

            collection.insert_one(x)

            t1 = threading.Thread(target=connect_sensor, args=(name,topic_w,topic_control,placeholder,))
            t1.start()


        #k= new_type['list_of_sensor_instances']
        #print (k[0])
