from bson.objectid import ObjectId
from pymongo import MongoClient
from kafka import KafkaConsumer, KafkaProducer
import json
import threading
import os

client = MongoClient("mongodb+srv://dhruv:abcd@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

SENSOR_DB_NAME = "sensor_registory"
BINDING_DB_NAME = "binding_db"
SENSOR_INSTANCES_COLLECTION_NAME = "sensor_instance"
SENSOR_MAP_COLLECTION_NAME = "sensor_map"
SENSOR_BINDER_TO_SCHEDULER_TOPIC = "sensor_binder_to_scheduler"

test_sensor_db = client[SENSOR_DB_NAME]
test_binding_db = client[BINDING_DB_NAME]
kafka_address = os.environ['KAFKA_ADDRESS']
#kafka_address = "localhost:9092"

def json_serializer(data):
   return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers=[kafka_address],
                        value_serializer=json_serializer)

class SensorBinder(object):
    def __init__(self, deploy_config):
        self.deploy_config = deploy_config

    def prepare_search_dict(self, dct):
        """
        Makes a dictionary which is used for searching on MongoDB
        """
        search_dict = {}
        search_dict['sensor_type'] = dct.get('sensor_type', "")
        
        filters = dct.get('filter_sensors', [])
        if filters:
            filters = filters[0]
            search_dict.update(filters)
        
        return search_dict

    def compare(self, dct1, dct2):
        return dct1 == dct2

    def sendDeployConfigToScheduler(self, instance_id):
        self.deploy_config["instance_id"] = instance_id
        producer.send(SENSOR_BINDER_TO_SCHEDULER_TOPIC, self.deploy_config)

    def processRequest(self):
        """
        First get the sensor info from config file
        bind the sensors with the info

        """
        sensor_info = self.deploy_config.get("sensor_info", {})
        result = self.bindSensors(sensor_info)
        if result["status_code"] != 200:
            print(result)
            return result

        instance_id = result["instance_id"]
        self.sendDeployConfigToScheduler(instance_id)
        return result

    def bindSensors(self, content):
        """
        Tries to bind the sensors and returns the status if the binding was done properly
        If binding is done, it returns an instance_id
        """
        
        n = len(content)
        if(n <= 0):
            return {"status_code": 500, "status": "error", "desc": "No sensors given in the config file to bind"}
        bit_map = [0]* n
        local_binding_map = {}
        for i in range(n):
            if bit_map[i] == 0:
                count = 1
                indices = [i]
                for j in range(i + 1, n):
                    if bit_map[j] == 0:
                        is_same = self.compare(content[i], content[j])
                        if is_same:
                            bit_map[j] = 1
                            count += 1
                            indices.append(j)

    #             print(i)
    #             print("indices: {}".format(indices))
    #             print(count)
                search_dict = self.prepare_search_dict(content[i])
    #             print(search_dict)
                num_matching_sensors = test_sensor_db[SENSOR_INSTANCES_COLLECTION_NAME].count_documents(search_dict)
                if num_matching_sensors < count:
                    return {"status_code": 501, "status": "error", "desc": "could not find as many sensors needed"}

                cursor = test_sensor_db[SENSOR_INSTANCES_COLLECTION_NAME].find(search_dict)

                for sensor_idx in range(count):
                    local_binding_map[str(indices[sensor_idx])] = cursor[sensor_idx]['_id']
                ## query database for content[i] and take the first count no of sensors from db
                
        print(local_binding_map)
        insert_result = test_binding_db[SENSOR_MAP_COLLECTION_NAME].insert_one(local_binding_map)
        if not insert_result.acknowledged:
            return {"status_code": 502, "status": "error", "desc": "could not write the binding data to db"}
        instance_id = str(insert_result.inserted_id)
        print(instance_id)
        return {"status_code": 200, "status": "OK", "instance_id": instance_id}

if __name__ == "__main__":
    consumer = KafkaConsumer(
       "pm_to_sensor_binder",
       bootstrap_servers=kafka_address,
       auto_offset_reset='earliest',
       group_id='consumer-group-a')

    print('Starting consumer to look out for deployconfig')
    for msg in consumer:
        deploy_config = json.loads(msg.value)
        print("deploy config recevied = {}\n".format(deploy_config))
        sensor_binder = SensorBinder(deploy_config)
        thread_id = threading.Thread(target=sensor_binder.processRequest)
        thread_id.start()
        
