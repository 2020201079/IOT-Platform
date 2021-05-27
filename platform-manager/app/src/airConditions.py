import sys
import platform_libfile
import time
from kafka.errors import KafkaError
from kafka import KafkaConsumer 
import json
def get_air_coditions():
    id = sys.argv[1]
    sensorkafkaname = platform_libfile.getSensorData(id,0)
    consumer = KafkaConsumer(sensorkafkaname,bootstrap_servers=['localhost:9092'],auto_offset_reset = "latest")
    for msg in consumer:
        msg = json.loads(msg.value)
        temp = msg["temperature"]
        print("Measuring temprature..."+temp)     
    


get_air_coditions()
