import json
import threading
import socket
from kafka import KafkaConsumer, KafkaProducer
from json import loads
import os
import requests
from pymongo import MongoClient
import time
import docker
import sys
import paramiko

topics = list()
data = {}
server_status = set()
auth_key = 'auth-key'
kakfa_broker = os.environ['KAFKA_ADDRESS'] # "52.151.195.26:9092"
kafka_address = os.environ['KAFKA_ADDRESS']

with open('config.json' , 'r') as vm_info:
    vmInfo = json.load(vm_info)


def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers=[kakfa_broker],
                        value_serializer=json_serializer)


class ServerLCDatabase:
    SERVER_DATABASE_NAME = "server_lifecycle"
    COLLECTION_NAME = "machine_info"
    TOPIC_KEY = 'topic'

####

class DeployConfigKeys:
    APPLICATION_NAME = "application_name"
    ALGORITHM_NAME = "algorithm_name"
    INSTANCE_ID = "instance_id"
    REQUEST_TYPE = "request_type"
    SCHEDULING_INFO = "scheduling_info"

class ServerDetailsKeys:
    NODE_NAME = "node_name"
    NODE_IP = "node_ip"

class MongoKeys:
    SLC_DB = "SLC_DB"
    INSTANCE_ID_TO_APP_ALGO = "instance_id_to_app_algo"
    INSTANCE_ID_TO_SERVER = "instance_id_to_server"


class LoadBalancer(object):
    def __init__(self):
        self.memory_usage_cmd = 'free | head -2 | tail -1 | awk \'{print $2, $3}\''
        self.cpu_idle_cmd = 'vmstat 1 2 | tail -1 | awk \'{print $15}\''
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def select_machine(self):
        """
        fetches load on all servers and returns machine name and IP of the least busy server
        """
        with open("server_details.json", "r") as fr:
            servers_details = json.load(fr)

        least_load = 1
        chosen_server_details = None

        for server in servers_details:
            try:
                server_details = servers_details[server]
                print(server_details)
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(server_details[ServerDetailsKeys.NODE_IP], username=server_details[ServerDetailsKeys.NODE_NAME])#password
                print("reached  here")
                _, mem_stdout, mem_stderr = ssh.exec_command(self.memory_usage_cmd)
                _, cpu_stdout, cpu_stderr = ssh.exec_command(self.cpu_idle_cmd)
                if(mem_stderr == '' or cpu_stderr == ''):
                    continue

                cpu_idle = cpu_stdout.readlines()
                mem_usage = mem_stdout.readlines()

                mem_usage_parts = mem_usage[0].split()
                mem_usage = int(mem_usage_parts[1][:-1]) / int(mem_usage_parts[0])

                cpu_usage = 1 - int(cpu_idle[0][:-1]) / 100
                load = 0.5*mem_usage + 0.5*cpu_usage
                print("load: {}".format(load))
                if(load <= least_load):
                    chosen_server_details = server_details
                    least_load = load

            except Exception as e:
                print(e)

        print(chosen_server_details)

        return chosen_server_details

load_balancer = LoadBalancer()

def build_docker_image(instance_id, DOCKERFILE_PATH, node_name, node_ip):
    print("building docker image now")
    
    DOCKER_FILE_DIR = os.path.dirname(DOCKERFILE_PATH)
    if DOCKER_FILE_DIR == "":
        DOCKER_FILE_DIR = "./"

    client = docker.DockerClient(base_url="ssh://{}@{}".format(node_name,node_ip))
    print("client made")
    try:    
        client.images.get(instance_id)
        print("Image {} already present".format(instance_id))
    except docker.errors.ImageNotFound:
        client.images.build(path=DOCKER_FILE_DIR, tag=instance_id)
        print("Image Built")
    except Exception as e:
        print("Some other exception occurred")
        print(e)


def run_container(instance_id, node_name, node_ip):
    print("running container now")
    client = docker.DockerClient(base_url="ssh://{}@{}".format(node_name, node_ip))

    try:
        container = client.containers.get(instance_id)
        print("Found a container already present. Restarting it")
        container.restart()
    except docker.errors.NotFound:

        print("Did not find the container. Creating one now")
        #autoremove,remove????????
        try:
            container = client.containers.run(instance_id, detach=True, network_mode="host", name=instance_id)
            print(container.id)
            # insert it into database?
        except Exception as e:
            print(e)
    
    except Exception as e:
        print("Something went wrong")
        print(e)

def start_app_instance(instance_id, app_name, algo_name):
    algo_path = "/datadrive/apps/{}/{}".format(app_name, algo_name)
    DOCKER_FILE_PATH = os.path.join(algo_path, 'dockerfile')

    # choosing the server
    chosen_server_details = load_balancer.select_machine()
    node_name = chosen_server_details["node_name"]
    node_ip = chosen_server_details["node_ip"]

    #build image and run container
    build_docker_image(instance_id, DOCKER_FILE_PATH, node_name, node_ip)
    run_container(instance_id, node_name, node_ip)

    # Inserting into db now
    mongo_client = MongoClient("mongodb+srv://dhruv:abcd@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    slc_db = mongo_client[MongoKeys.SLC_DB]
    key = {'instance_id': instance_id}
    data_dict = {'instance_id': instance_id, 'app_name': app_name, 'algo_name': algo_name}
    slc_db[MongoKeys.INSTANCE_ID_TO_APP_ALGO].update(key, data_dict, upsert=True)


    server_key = {'instance_id': instance_id}
    server_data_dict = {'instance_id': instance_id, 'node_name': node_name, 'node_ip': node_ip}
    slc_db[MongoKeys.INSTANCE_ID_TO_SERVER].update(server_key, server_data_dict, upsert=True)

def restart_app(instance_id):
    mongo_client = MongoClient("mongodb+srv://dhruv:abcd@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    slc_db = mongo_client[MongoKeys.SLC_DB]
    query_dict = {'instance_id': instance_id}
    num_documents = slc_db[MongoKeys.INSTANCE_ID_TO_APP_ALGO].count_documents(query_dict)
    if(num_documents <= 0):
        print("Could not find any matching instance that ran before: {}".format(instance_id))
        return
    
    cursor = slc_db[MongoKeys.INSTANCE_ID_TO_APP_ALGO].find(query_dict)
    document = cursor[0]
    app_name = document['app_name']
    algo_name = document['algo_name']
    start_app_instance(instance_id, app_name, algo_name)

def stop_app(instance_id, node_name, node_ip):
    try:
        client = docker.DockerClient(base_url="ssh://{}@{}".format(node_name,node_ip))
        container = client.containers.get(instance_id)
        container.stop()
        container.remove()
        mongo_client = MongoClient("mongodb+srv://dhruv:abcd@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
        slc_db = mongo_client[MongoKeys.SLC_DB]
        query_dict = {'instance_id': instance_id}
        slc_db[MongoKeys.INSTANCE_ID_TO_SERVER].delete_one(query_dict)
    except docker.errors.NotFound as e:
        print("Did not find such a container")
        print(e)
    except Exception as e:
        print(e)

def stop_app_instance(instance_id):
    mongo_client = MongoClient("mongodb+srv://dhruv:abcd@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    slc_db = mongo_client[MongoKeys.SLC_DB]
    query_dict = {'instance_id': instance_id}
    num_documents = slc_db[MongoKeys.INSTANCE_ID_TO_SERVER].count_documents(query_dict)
    if num_documents <= 0:
        print("No app instance to stop")
        return
    else:
        cursor = slc_db[MongoKeys.INSTANCE_ID_TO_SERVER].find(query_dict)
        result = cursor[0]
        node_name = result['node_name']
        node_ip = result['node_ip']
        stop_app(instance_id, node_name, node_ip)

def sendRequestToServer(deploy_config_file):
    instance_id = deploy_config_file[DeployConfigKeys.INSTANCE_ID]
    app_name = deploy_config_file[DeployConfigKeys.APPLICATION_NAME]
    algo_name = deploy_config_file[DeployConfigKeys.ALGORITHM_NAME]
    request_type = deploy_config_file[DeployConfigKeys.SCHEDULING_INFO][DeployConfigKeys.REQUEST_TYPE]
    
    if(request_type == "start"):
        start_app_instance(instance_id, app_name, algo_name)
    elif request_type == "stop":
        stop_app_instance(instance_id)

def deployer_request():

    global kakfa_broker

    kafka_address = kakfa_broker

    args = sys.argv
    if(len(args) > 1):
        deploy_config_file_path = args[1]
    
        with open(deploy_config_file_path, 'r') as fr:
            deploy_config_file = json.load(fr)

        sendRequestToServer(deploy_config_file)

    else:

        deployer_to_slc_consumer = KafkaConsumer(
        "deployer_to_slc",
        bootstrap_servers=kafka_address,
        auto_offset_reset='earliest',
        group_id='consumer-group-a')
        
        for msg in deployer_to_slc_consumer:
            deploy_request = json.loads(msg.value)
            print("----")
            print(type(deploy_request))
            print(deploy_request)
            th = threading.Thread(target=sendRequestToServer, args=(deploy_request, ))
            th.start()

def restart_app_handler():
    restart_app_consumer = KafkaConsumer(
        "app_monitoring_to_restart",
        bootstrap_servers=kafka_address,
        auto_offset_reset='earliest',
        group_id='consumer-group-a')

    for msg in restart_app_consumer:
        restart_app_request = json.loads(msg.value)
        instance_id = restart_app_request['instance_id']
        print(instance_id)
        print("----")
        print(type(restart_app_request))
        restart_thread = threading.Thread(target=restart_app, args=(instance_id,))
        restart_thread.start()
###

def readServerInfo():

    global topics
    global data
    global cluster

    cluster = MongoClient("mongodb+srv://dhruv:abcd@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

    db = cluster[ServerLCDatabase.SERVER_DATABASE_NAME]
    collection = db[ServerLCDatabase.COLLECTION_NAME]
    cursor = collection.find({})
    for document in cursor:
        if document[ServerLCDatabase.TOPIC_KEY] not in topics:
            topics.append(document[ServerLCDatabase.TOPIC_KEY])

def restart_machine(machine_ip):
    # Read machine details from config.json
    subscription_id = vmInfo['machines'][machine_ip]["subscription_id"]
    machine_name = vmInfo['machines'][machine_ip]["machine_name"]
    resource_group_name = vmInfo['machines'][machine_ip]["resource_group_name"]
    user_name = vmInfo['machines'][machine_ip]["username"]

    # Send request to start machine
    start_machine(subscription_id,machine_name,resource_group_name)

    # Run machine_status_checker after restart
    try:
        cmd = "ssh "+user_name+"@"+machine_ip + " \"export KAFKA_ADDRESS=" + kafka_broker +" && python3 ~/machine_status_checker/machine_status_checker.py &\""
        print(cmd)
        os.system(cmd)
    except Exception as e:
        print("Could not start machine_status_checker")
        print(e)

    # call function to restart instance_ids

def consumer_handler(server_topic):

    global server_status
    global kakfa_broker
    print('consuming from: ', server_topic)
    consumer = KafkaConsumer(
        server_topic,
        bootstrap_servers= kakfa_broker,
        auto_offset_reset='earliest',
        group_id='server-status',
        consumer_timeout_ms=80000)

    # Consume message from machine
    for message in consumer:
        message = message.value
        print(server_topic,':',message)

    # If reached here, no status recieved , restart machine
    server_status.remove(server_topic)
    consumer.close()
    
    print("***Restarting Server:",server_topic,'***')
    restart_machine(server_topic[:-7]) # "ip_status" s[:-7]

def start_machine(subscription_id,machine_name,resource_group_name):
    print(subscription_id,machine_name,resource_group_name)
    url = "https://management.azure.com/subscriptions/" + subscription_id + "/resourceGroups/" + resource_group_name +  "/providers/Microsoft.Compute/virtualMachines/" + machine_name + "/start?api-version=2020-12-01"
    print(url)
    response = requests.post(url, headers={"Authorization" : vmInfo[auth_key]})
    print(response)

def stop_machine(subscription_id,machine_name,resource_group_name):
    url = "https://management.azure.com/subscriptions/" + subscription_id + "/resourceGroups/" + resource_group_name +  "/providers/Microsoft.Compute/virtualMachines/" + machine_name + "/poweroff?api-version=2020-12-01"
    response = requests.post(url, headers={"Authorization" : vmInfo[auth_key]})

def check_server_status():

    global topics

    readServerInfo()

    for i in topics:

        # if server is already running do not create new thread
        if i in server_status:
            continue

        server_status.add(i)
        consumer_thread = threading.Thread(target=consumer_handler,args=(i,))
        consumer_thread.start()

def check():
    print('started checking status of servers')
    while(1):
        check_server_status()

def main():
    print('started server lifecycle manager')

    # Thread to check the machine status
    #status = threading.Thread(target=check,args=())
    #status.start()

    #Thread to listen to deployer request
    deploy = threading.Thread(target=deployer_request,args=())
    deploy.start()

    #Thread app monitoring
    restart = threading.Thread(target=restart_app_handler, args=())
    restart.start()

if __name__ == "__main__":
    main()
