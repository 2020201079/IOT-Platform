import sys
import platform_libfile
import time
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import KafkaConsumer


def get_water_content():
    id = sys.argv[1]
    humiditykafkaname = platform_libfile.getSensorData(id,0)

    consumer = KafkaConsumer(humiditykafkaname,bootstrap_servers=['localhost:9092'],auto_offset_reset = "latest")
    # producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'])

    for msg in consumer:
        msg = json.loads(msg.value)
        humidity = msg["moisture"]
        print("Measuring humidity..."+humidity)
        if(humidity < 5):
            print("Humidity low sprinkler started...")
            # stat = platform_libfile.setSensorData(id,1)
            # if(stat):
            #     print("Sprinkler Started...")
            # else:
            #     print("Failed to start the sprinklers")
    

get_water_content()