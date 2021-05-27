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
    humidityControlkafkaname = platform_libfile.setSensorData(id,0)
    print("topic name : ",humiditykafkaname)
    print("topic control name : ",humidityControlkafkaname)
    consumer = KafkaConsumer(humiditykafkaname,bootstrap_servers=['52.146.2.26:9092'],auto_offset_reset = "latest")
    producer = KafkaProducer(bootstrap_servers=['52.146.2.26:9092'])

    for msg in consumer:
        #msg = json.loads(msg.value)
        humidity = float(msg.value.decode('utf-8'))
        print("Measuring humidity..."+str(humidity))
        if(humidity < 5):
            print("Humidity low sprinkler started...")
            producer.send(humidityControlkafkaname,"1".encode())
        if(humidity > 15):
            print("Humidity controlled sprinkler is stopped...")
            producer.send(humidityControlkafkaname,"0".encode())
            # stat = platform_libfile.setSensorData(id,1)
            # if(stat):
            #     print("Sprinkler Started...")
            # else:
            #     print("Failed to start the sprinklers")
    

get_water_content()
