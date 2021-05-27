"""
Needs an updated config.json

"""

import os
import docker
import json
import time
import threading
from pymongo import MongoClient

from kafka import KafkaConsumer, KafkaProducer

kafka_address = os.environ['KAFKA_ADDRESS']


class MongoKeys:
    SLC_DB = "SLC_DB"
    INSTANCE_ID_TO_APP_ALGO = "instance_id_to_app_algo"
    INSTANCE_ID_TO_SERVER = "instance_id_to_server"
    SERVICE_LIFECYCLE_DB = "service_lifecycle"
    SERVICE_STATUS = "service_status"

def json_serializer(data):
        return json.dumps(data).encode('utf-8')

def send_status():
    
    with open("config.json", "r") as fr:
        config_dict = json.load(fr)

    config_dict = config_dict['machines']
    while True:
        
        containers_status = dict()
        try:
            mongo_client = MongoClient("mongodb+srv://dhruv:abcd@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
            slc_db = mongo_client[MongoKeys.SLC_DB]
            collection = slc_db[MongoKeys.INSTANCE_ID_TO_SERVER]
            cursor = collection.find({})
            for item in cursor:
                try:
                    instance_id = item['instance_id']
                    node_name = item['node_name']
                    node_ip = item['node_ip']
                    print(instance_id, node_name, node_ip)
                
                    client = docker.DockerClient(base_url="ssh://{}@{}".format(node_name, node_ip))

                    cont = client.containers.get(instance_id)
                    cont_status = cont.status
                    containers_status[instance_id] = cont_status
                except Exception as e:
                    print("Could not find any such container")

            service_lifecycle_db = mongo_client[MongoKeys.SERVICE_LIFECYCLE_DB]
            collection = service_lifecycle_db[MongoKeys.SERVICE_STATUS]
            cursor = collection.find({})
            for item in cursor:
                try:
                    instance_id = item['container_id']
                    node_ip = item['ip']
                    print(instance_id, node_ip)
                    node_name = config_dict[node_ip]['username']
                    

                    client = docker.DockerClient(base_url="ssh://{}@{}".format(node_name, node_ip))

                    cont = client.containers.get(instance_id)
                    cont_status = cont.status
                    containers_status[instance_id] = cont_status
                except Exception as e:
                    print("Could not find any such container")
                    
            
            producer = KafkaProducer(bootstrap_servers=[kafka_address],
                    value_serializer=json_serializer)
            print("sending containers status dictionary")
            producer.send('app_monitoring_to_dashboard', containers_status)

        except Exception as e:
            print("Could not have connected to the server")
            print(e)

        time.sleep(60)

def send_logs():
    consumer = KafkaConsumer(
        "dashboard_to_log",
        bootstrap_servers=kafka_address,
        auto_offset_reset='earliest',
        group_id='consumer-group-a')

    with open("config.json", "r") as fr:
        config_dict = json.load(fr)

    config_dict = config_dict['machines']
    
    for msg in consumer:
        print("Fetch logs request received: {}".format(msg.value))
        #print(type(msg.value))
        instance_id = msg.value.decode()
        #print(type(instance_id))
        print(instance_id)
        print("[Fetch logs] for instance_id : {}".format(instance_id))
        mongo_client = MongoClient("mongodb+srv://dhruv:abcd@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
        slc_db = mongo_client[MongoKeys.SLC_DB]
        collection = slc_db[MongoKeys.INSTANCE_ID_TO_SERVER]
        query_dict = {'instance_id': instance_id}
        cursor = collection.find_one(query_dict)
        cont_logs = dict()
        cont_logs[instance_id] = ""
        

        if cursor:
            node_name = cursor['node_name']
            node_ip = cursor['node_ip']
            ## node_name, node_ip = 
            try:
                print("[Fetch logs] Connecting to docker now")
                client = docker.DockerClient(base_url="ssh://{}@{}".format(node_name, node_ip)) ##??
                cont = client.containers.get(instance_id)
                # producer = KafkaProducer(bootstrap_servers=[kafka_address],
                #                 value_serializer=json_serializer)
                
                cont_logs[instance_id] = str(cont.logs())
                # producer.send('log_to_dashboard', cont_logs)
            except Exception as e:
                print("Could not have connected to the server")
                print(e)

        else:
            mongo_client = MongoClient("mongodb+srv://dhruv:abcd@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
            service_lifecycle_db = mongo_client[MongoKeys.SERVICE_LIFECYCLE_DB]
            collection = service_lifecycle_db[MongoKeys.SERVICE_STATUS]
            cursor = collection.find_one({'container_id': instance_id})
            if cursor:
                try:
                    node_ip = cursor['ip']
                    node_name = config_dict[node_ip]['username']
                    client = docker.DockerClient(base_url="ssh://{}@{}".format(node_name, node_ip))
                    cont = client.containers.get(instance_id)
                    cont_logs[instance_id] = str(cont.logs())
                except Exception as e:
                    print("Could not have connected to the server")
                    print(e)
            
        producer = KafkaProducer(bootstrap_servers=[kafka_address],
                            value_serializer=json_serializer)
        print("[Fetch logs] sending logs now")
        #print(cont_logs)
        producer.send('log_to_dashboard', cont_logs)
        


if __name__ == "__main__":
    # with open("server_details.json", "r") as fr:
    #     servers_details = json.load(fr)

    send_logs_thread = threading.Thread(target=send_logs, args=())
    send_logs_thread.start()

    send_status_thread = threading.Thread(target=send_status, args=())
    send_status_thread.start()
