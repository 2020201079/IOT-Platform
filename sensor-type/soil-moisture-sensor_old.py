import sys
from faker import Faker
from kafka import KafkaProducer
import json
import time
import threading
import os
from kafka import KafkaConsumer

def json_serializer(data):
    return data.encode()

def get_partition(key,all,available):
    return 0

fake = Faker()

def get_data():
    return {
        "humidity": fake.pydecimal()
    }

kafka_address = os.environ['KAFKA_ADDRESS']

producer = KafkaProducer(bootstrap_servers=[kafka_address],
                         value_serializer=json_serializer)
control_topic = sys.argv[2]

#control function
def set_data(data):
	if(data == 1):
		print('Sprinkler On')
	elif(data == 0):
		print('Sprinkler Off')
	else:
		print('Invalid Input')

def consumer_thread():
	consumer = KafkaConsumer(control_topic,
        bootstrap_servers=[kafka_address],
        auto_offset_reset='earliest',
        group_id='consumer-group-a')
        
	for msg in consumer:
		set_data(int(msg.value.decode()))

if __name__=='__main__':
    topic_name = sys.argv[1]
    threading.Thread(target=consumer_thread, args=()).start()
    
    while 1 == 1:
        registered_user = get_data()
        #print(registered_user["humidity"])
        producer.send(topic_name,str(registered_user["humidity"]))
        time.sleep(5)
