"""
kafka consume for cases when a VM has restarted

Baaki- for montoring: server_details.json. 
For all machines, keep checking if there is a container with exit status 137. If found, restart.

docker build -t app_monitoring .
docker run -v ~/.ssh:/root/.ssh --net=host app_monitoring
"""

import os
import docker
import json
import time

from kafka import KafkaConsumer, KafkaProducer

kafka_address = os.environ['KAFKA_ADDRESS']


def json_serializer(data):
        return json.dumps(data).encode('utf-8')

if __name__ == "__main__":
    with open("server_details.json", "r") as fr:
        servers_details = json.load(fr)

    while True:
        for server_no in servers_details:
            server_details = servers_details[server_no]
            node_name = server_details['node_name']
            node_ip = server_details['node_ip']

            try:
                client = docker.DockerClient(base_url="ssh://{}@{}".format(node_name, node_ip))
                containers = client.containers.list(all=True, filters={'exited': [130, 137, 143]})
                for cont in containers:
                    instance_id = cont.name
                    producer = KafkaProducer(bootstrap_servers=[kafka_address],
                        value_serializer=json_serializer)
                    print("sending restart request for cont name: {}".format(instance_id))
                    producer.send('app_monitoring_to_restart', {'instance_id': instance_id})
            
            except Exception as e:
                print("Could not have connected to the server")
                print(e)
        
        time.sleep(60)



                


