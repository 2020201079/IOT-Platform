# For now I am fxing the ip address of each node.
# all machines can perform password less ssh among each other

# all machines have common folder /datadrive --> created using nfs

from paramiko import SSHClient
import paramiko
import pymongo
from pymongo import MongoClient

host1 = '52.188.83.232'
username1 = 'rootadmin'

host2 = '20.62.200.216'
username2 = 'rootadmin'

host3 = '20.84.81.153'
username3 = 'rootadmin'

kafka_address = host1+':9092'

pathToPlatform = '/datadrive/bootstrap/ias-spring-2021-group-4/sample_application/'


class ServiceLCDatabase:
    DATABASE_NAME = "service_lifecycle"
    COLLECTION_NAME = "service_status"
    SERVICE_NAME = "_id"
    CONTAINER_ID = "container_id"
    SERVICE_STATUS_INACTIVE = "INACTIVE"
    SERVICE_STATUS_ACTIVE = "ACTIVE"

def establish_db_connection():
    print('Establishing DB connection')
    cluster = MongoClient("mongodb+srv://dhruv:abcd@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = cluster[ServiceLCDatabase.DATABASE_NAME]
    collection = db[ServiceLCDatabase.COLLECTION_NAME]
    print('DB connection established')
    return collection

def insertInDB(collection,image_name,container_id,ip,status):
    document = {
        "_id" : image_name,
        "container_id" : container_id,
        "ip" : ip,
        "status" : status
    }

    try:
        collection.update_one({'_id':image_name},{"$set": document}, upsert=True)
        #collection.insert_one(document,upsert=True)
    except pymongo.errors.DuplicateKeyError:
        pass

def makeSSHClient(host1,username1):
    ssh1 = SSHClient()
    ssh1.load_system_host_keys()
    ssh1.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh1.connect(hostname=host1,port=22,username=username1)
    return ssh1

def createDockerImage(ssh,ImageName,folderName): #path -> platformManager_docker
    dockerFilePath = pathToPlatform+folderName+'/dockerfile'
    pathToFolder = pathToPlatform+folderName+'/'
    command = 'docker build -t {} -f {} {}'.format(ImageName,dockerFilePath,pathToFolder)
    print('command is : ',command)
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(command)
    print(ssh_stdout.readlines())


collection = establish_db_connection()

ssh1 = makeSSHClient(host1,username1)
ssh2 = makeSSHClient(host2,username2)
ssh3 = makeSSHClient(host3,username3)

# server life cycle
# first run machine_status_checker.py in all machines
# run serverLifeCycle.py in node 1

#Platform manager
image_name = 'platform-manager'
createDockerImage(ssh1,image_name,'platformManager_docker')
dockRunCommand_platform = 'docker run -d --name platform-manager -v /datadrive:/datadrive -p 5001:5001 -e KAFKA_ADDRESS={} platform-manager'.format(kafka_address)
ssh_stdin, ssh_stdout, ssh_stderr = ssh1.exec_command(dockRunCommand_platform)
print(ssh_stdout.readlines())
insertInDB(collection,image_name,image_name,host1,'active')


#Sensor type 
image_name = 'sensor-type'
createDockerImage(ssh1,'sensor-type','Sensor_Management_type_docker')
dockRunCommand_sensor_type = 'docker run -d --name sensor-type -e KAFKA_ADDRESS={} sensor-type '.format(kafka_address)
ssh_stdin, ssh_stdout, ssh_stderr = ssh1.exec_command(dockRunCommand_sensor_type)
print(ssh_stdout.readlines())
insertInDB(collection,image_name,image_name,host1,'active')

# #Sensor instance
image_name = 'sensor-instance'
createDockerImage(ssh1,'sensor-instance','Sensor_Management_instance_docker')
dockerRunCommand_sensor_instance = 'docker run -d --name sensor-instance -e KAFKA_ADDRESS={} sensor-instance'.format(kafka_address)
ssh_stdin, ssh_stdout, ssh_stderr = ssh1.exec_command(dockerRunCommand_sensor_instance)
print(ssh_stdout.readlines())
insertInDB(collection,image_name,image_name,host1,'active')