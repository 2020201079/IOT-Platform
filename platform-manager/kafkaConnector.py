from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import os

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

class kafkaConnector:
    def __init__(self):
        kafka_address = os.environ['KAFKA_ADDRESS']
        self.producer = KafkaProducer(bootstrap_servers=[kafka_address],
                         value_serializer=json_serializer)
    
    def sendJsonData(self,topicName,data):
        self.producer.send(topicName,data)
    
    def getJsonData(self,topicName):
        consumer = KafkaConsumer(
                                topicName,
                                bootstrap_servers=kafka_address,
                                auto_offset_reset='earliest')
        print('starting the consumer')
        for msg in consumer:
            print("data = {}".format(json.loads(msg.value)))
    

if __name__=="__main__":
    kafka = kafkaConnector()
    '''
    with open('./sensorTypeRegistration.json') as f:
        data = json.load(f)
        kafka.sendJsonData('pm_to_sensor_type_reg',data)
    '''
    kafka.getJsonData('pm_to_sensor_type_reg')
