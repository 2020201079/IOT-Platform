from kafka import KafkaProducer,KafkaConsumer
from pymongo import MongoClient
import pymongo
import json
import requests
import time

kakfa_broker = os.environ['KAFKA_ADDRESS']
ip_address = os.environ['IP_ADDRESS']

def kafka_producer(id,ip,topic):
    producer = KafkaProducer(bootstrap_servers = kakfa_broker,value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    while(1):
        data = {'number' : id ,'ip': ip} 
        producer.send(topic,value=data)
        time.sleep(5)


ip = ip_address # str(requests.get('https://checkip.amazonaws.com').text.strip())


print('my_ip:', ip)

machine_name = "VM_" + ip

topic_name = ip + '_status'

content = { "_id" : machine_name,
            "ip" : ip,
            "topic" : topic_name
        }

cluster = MongoClient("mongodb+srv://dhruv:abcd@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = cluster["server_lifecycle"]
collection = db["machine_info"]

try:
    collection.insert_one(content)
except pymongo.errors.DuplicateKeyError:
    pass

kafka_producer(machine_name,ip,topic_name)
