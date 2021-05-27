import json
from kafka import KafkaConsumer, KafkaProducer
import docker
import pymongo
from pymongo import MongoClient
import os
import time
import threading
import requests
import paramiko
import socket
'''
print(service_info) # [{'_id': 'dummy_service', 'container_id': '5d8c57cdf7', 'ip': '127.0.0.1', 'status': 'Inactive'}]
'''

CONFIG_FILE_PATH = '/datadrive/bootstrap/ias-spring-2021-group-4/sample_application/service_lifecycle/config.json'
KAFKA_BROKER = os.environ['KAFKA_ADDRESS'] # "52.151.195.26:9092" # 
IP_ADDRESS = os.environ['IP_ADDRESS']

ip_to_client_map = dict()

class ServiceLCDatabase:
    DATABASE_NAME = "service_lifecycle"
    COLLECTION_NAME = "service_status"
    SERVICE_NAME = "_id"
    CONTAINER_ID = "container_id"
    SERVICE_STATUS_INACTIVE = "INACTIVE"
    SERVICE_STATUS_ACTIVE = "ACTIVE"

class DockerfilePath:
    SOURCE = "/datadrive/bootstrap/ias-spring-2021-group-4/sample_application/"
    TARGET = "/datadrive/bootstrap/ias-spring-2021-group-4/sample_application/"

class ServerDetailsKeys:
    NODE_NAME = "node_name"
    NODE_IP = "node_ip"

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

def establish_db_connection():
    print('Establishing DB connection')
    cluster = MongoClient("mongodb+srv://dhruv:abcd@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = cluster[ServiceLCDatabase.DATABASE_NAME]
    collection = db[ServiceLCDatabase.COLLECTION_NAME]
    print('DB connection established')
    return collection

def get_all_services_info(collection):
    
    service_names = collection.find().distinct(ServiceLCDatabase.SERVICE_NAME)
    service_info = list()

    for service in service_names:
        service_info.append(collection.find_one(service))
    
    return service_info


def update(service_name, collection, only_status = False, status = None, ip = None, container_id = None):

    if only_status == False:
        collection.update_many({'_id':service_name},
                                {'$set': {'status': status,"ip":ip,"container_id":container_id
                                }})
    else:
        collection.update_many({'_id':service_name},
                                    {'$set': {'status': status
                                }})


def restart_service(service_name, old_container_id, user_name_ip_map,ip_to_client_map):
    
    #get ip from load balancer
    load_balancer = LoadBalancer()
    chosen_server_details = load_balancer.select_machine()
    node_name = chosen_server_details["node_name"]
    ip = chosen_server_details["node_ip"]

    print('chosen ip:',ip)

    # ip = "20.51.195.189" #change
    
    user_name = user_name_ip_map[ip]

    docker_client = ip_to_client_map[ip]

    
    source = " -f " + DockerfilePath.SOURCE + service_name + "/Dockerfile"
    target = DockerfilePath.TARGET + service_name
    prefix = "ssh " + user_name + "@" + ip + " "

    #docker build
    try:
        cmd_build = prefix + "docker build -t "+ service_name + source + " " + target
        os.system(cmd_build)
    except docker.errors.BuildError:
        print("No image found")

    #Remove old container if conflict
    try:
        cmd_remove = prefix + "docker container rm " + service_name
        os.system(cmd_remove)
    except Exception as e:
        print('cannot delete ', old_container_id, ' does not exist in machine:',ip, ' continuing...')

    # Run docker
    host_ssh_drive = "/home/rootadmin/.ssh"
    new_container = docker_client.containers.run(
        service_name,detach=True,
        name=service_name,
        environment={"KAFKA_ADDRESS":KAFKA_BROKER},
        ports = {'5001/tcp': 5001},
        volumes = {'/datadrive': {'bind': '/datadrive', 'mode': 'rw'},
                  host_ssh_drive: {'bind': '/root/.ssh', 'mode': 'rw'}}
        )

    new_container_id = new_container.id 

    return ip, new_container_id


def check_service_status(ip, collection, client, service_info, user_name_ip_map,ip_to_client_map): #(collection, client,service_info, user_name_ip_map,)
    
    # service_info = get_all_services_info(collection)
    connection_error = False

    while True and connection_error == False:
        # container_list = client.containers.list(all = True , filters = {'exited' : '137'})
        try:
            container_list = client.containers.list(all = True)
        except socket.error:
            print('Remote machine unreachable:',ip)
            connection_error = True
            break

        container_short_id = list()
        
        for container in container_list:
            try:
                container_id = container.id
                container_short_id.append(container_id)
            except Exception as e:
                print('Invalid container : ',container)
                print(e)
                continue
        

        for service in service_info:
            service_name = service[ServiceLCDatabase.SERVICE_NAME]
            
            try:
                container_id = service[ServiceLCDatabase.CONTAINER_ID]# client.containers.get(service_name).short_id
            except Exception as e:
                print('Invalid service name:',service_name)
                print(e)
                continue

            if container_id in container_short_id:
                print('container_id:',container_id)
                status = None
                try:
                    container_status = client.containers.get(container_id).status
                    if container_status == 'exited' or container_status == 'created':
                        print("Restarting container : ", container_id, ' service:',service_name)
                        status = ServiceLCDatabase.SERVICE_STATUS_INACTIVE
                        print(status)
                        # update status to Inactive in DB
                        print('update status to Inactive in DB')
                        update(service_name, collection , only_status=True, status=status)
                        time.sleep(20)
                        # restart
                        print('restart')
                        ip, new_container_id = restart_service(service_name, container_id, user_name_ip_map,ip_to_client_map)

                        # update status to active, new ip, new container_id in DB
                        status = ServiceLCDatabase.SERVICE_STATUS_ACTIVE
                        print('update status to active, new ip, new container_id in DB:',new_container_id)
                        time.sleep(20)
                        update(service_name, collection , status=status,ip=ip,container_id=new_container_id)
                        service['container_id'] = new_container_id
                    else:
                        print("Running:",container_id)

                except socket.error:
                    print('Remote machine unreachable:',ip)
                    connection_error = True
                    break
                except docker.errors.NotFound:
                    pass
                    # print('Invalid Container ID')
        time.sleep(10)
    
    if connection_error == True:
        print('Waiting Response from Remote machine:',ip)
        status = ServiceLCDatabase.SERVICE_STATUS_INACTIVE
        update(service_name, collection , only_status=True, status=status)
        time.sleep(120)
        check_status(ip, collection, service_info, user_name_ip_map,)


def check_status(ip, collection, service_info, user_name_ip_map,):

    my_ip = IP_ADDRESS # str(requests.get('https://checkip.amazonaws.com').text.strip())

    if my_ip == ip:
        client = docker.from_env()
    else:
        docker_url = "ssh://" + user_name_ip_map[ip] + "@" + ip
        client = docker.DockerClient(base_url=docker_url, timeout=180)

    ip_to_client_map[ip] = client
    check_service_status(ip, collection, client,service_info, user_name_ip_map,ip_to_client_map)

def main():
    print('started service lifecycle manager')

    with open(CONFIG_FILE_PATH , 'r') as config:
        machine_info = json.load(config)

    user_name_ip_map = dict()

    ip_address = machine_info['machines'].keys()

    for ip in ip_address:
        user_name_ip_map[ip] = machine_info['machines'][ip]['username']

    collection = establish_db_connection()
    service_info = get_all_services_info(collection)

    for ip in user_name_ip_map:
        docker_client_thread = threading.Thread(target=check_status,args=(ip, collection, service_info, user_name_ip_map,)) # check_service_status(collection, service_info):
        docker_client_thread.start()


    # client = docker.from_env()
    # docker_thread = threading.Thread(target=check_service_status,args=(client, collection, service_info,)) # check_service_status(collection, service_info):
    # docker_thread.start()

if __name__ == "__main__":
    main()



