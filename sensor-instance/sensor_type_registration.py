from kafka import KafkaConsumer
import json
import json
import os
from pymongo import MongoClient

cluster = MongoClient("mongodb+srv://shweta_10:shweta10@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = cluster["sensor_registory"]
collection = db["sensor_type"]

if __name__=='__main__':
    topic_name = "pm_to_sensor_type_reg"
    kafka_address=os.environ['KAFKA_ADDRESS']
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[kafka_address],
        auto_offset_reset='earliest',
        group_id='consumer-group-c')
    print('starting the consumer')
    for msg in consumer:
        new_type = json.loads(msg.value)
        print(type(new_type))
        k = new_type["sensor_type_list"]
        for x in k:
            collection.insert_one(x)
