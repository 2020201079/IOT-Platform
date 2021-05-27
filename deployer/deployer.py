"""
Changes to be done: Write the files deployconfig, dockerfile and requirements.txt into a folder named instance_id
and then send the path only to serverlife cycle manager
send instance_id key to serverlife cycle manager

docker run -v /datadrive:/datadrive --net=host deployer
"""

#!/usr/bin/python3
from kafka import KafkaConsumer
from kafka import KafkaProducer
import threading
import json
import os
import sys
from shutil import copyfile

kafka_address = os.environ['KAFKA_ADDRESS']
#kafka_address = "localhost:9092"
 
class Deployer:
    def __init__(self, deploy_config_file):
        self.deploy_config_file = deploy_config_file
        self.files = {"deploy_config_file":deploy_config_file}
        self.LIBFILE_SRC_PATH = "/datadrive/platform_files/platform_libfile.py"

    def json_serializer(self,data):
        return json.dumps(data).encode('utf-8')

    # def create_docker_file(self):
    #     # lang = self.deploy_config_file["environment"]["lang"] ## NOT USED .
    #     script = self.deploy_config_file["script_name"]
    #     instance_id = self.deploy_config_file["instance_id"]
    #     # docker_file = open("dockerfile","w")
    #     # lang = self.deploy_config_file["environment"]["lang"] ## NOT USED .
    #     # script = self.deploy_config_file["script_name"]
    #     script_paths = self.deploy_config_file["script_file_path"]
    #     add_script_str = ""
    #     for file_path in script_paths:
    #         file_name = os.path.basename(file_path)
    #         add_script_str += "ADD {} .\n".format(file_name)
            

    #     # docker_file = "FROM python:3\n" + "COPY requirements.txt ./\n" + "RUN pip install --upgrade pip\n" + "RUN pip install --no-cache-dir -r requirements.txt\n" + "ADD "+script+" .\n" + "CMD [\"python\", \"-u\"," + "\"{}\",".format(script)+  "\"{}\"".format(instance_id) + "]"
    #     docker_file = "FROM python:3\n" + "COPY requirements.txt ./\n" + "RUN pip install --upgrade pip\n" + "RUN pip install --no-cache-dir -r requirements.txt\n" + add_script_str + "CMD [\"python\", \"-u\"," + "\"{}\",".format(script)+  "\"{}\"".format(instance_id) + "]"
         
    #     self.files["docker_file"] = docker_file

    def create_docker_file(self):
        app = self.deploy_config_file["application_name"]
        scripts = self.deploy_config_file["script_names"]
        algorithm = self.deploy_config_file["algorithm_name"]
        instance_id = self.deploy_config_file["instance_id"]

        self.ALGO_PATH = "/datadrive/apps/{}/{}/".format(app,algorithm)
        ALGO_PATH = self.ALGO_PATH
        LIBFILE_DEST_PATH  = os.path.join(ALGO_PATH,"platform_libfile.py")

        DOCKERFILE_PATH = os.path.join(ALGO_PATH,"dockerfile")

        # if(os.path.exists(DOCKERFILE_PATH)):
        #     return

        if(not os.path.exists(LIBFILE_DEST_PATH)):
            copyfile(self.LIBFILE_SRC_PATH,LIBFILE_DEST_PATH)
        
        add_script_str = ""
        for file_name in os.listdir(ALGO_PATH):
            add_script_str += "ADD {} .\n".format(file_name)


        add_scripts = ""
        for file_name in scripts:
            add_scripts += "CMD [\"python\", \"-u\"," + "\"{}\",".format(file_name)+  "\"{}\"".format(instance_id) + "]\n"

        docker_file = "FROM python:3\n" + "COPY requirements.txt ./\n" + "RUN pip install --upgrade pip\n" + "RUN pip install --no-cache-dir -r requirements.txt\n" + "ENV KAFKA_ADDRESS " + kafka_address + "\n" +  add_script_str + add_scripts

        with open(DOCKERFILE_PATH,"w") as f:
            f.write(docker_file)
        

    def create_req_file(self):
        # req_file = open("requirements.txt","w")
        req_file = ""

        REQFILE_PATH = os.path.join(self.ALGO_PATH,"requirements.txt")

        dependencies = self.deploy_config_file["environment"]["dependencies"]

        for dependency in dependencies:
            if(dependency[1]!=""):
                req_file += dependency[0]+"=="+dependency[1]+"\n"
            else:
                req_file += dependency[0]+"\n"
        with open(REQFILE_PATH,"w") as f:
            f.write(req_file)
        # self.files["req_file"] = req_file


    def create_files(self):
        self.create_docker_file()
        self.create_req_file()
        producer = KafkaProducer(bootstrap_servers=[kafka_address],
                        value_serializer=self.json_serializer)
        producer.send('deployer_to_slc', self.deploy_config_file)

    def send_stop_request(self):
        producer = KafkaProducer(bootstrap_servers=[kafka_address],
                        value_serializer=self.json_serializer)
        producer.send('deployer_to_slc', self.deploy_config_file)



if __name__=='__main__':
    args = sys.argv
    if(len(args) > 1):
        with open(args[1],"r") as f:
            deploy_config_file = json.load(f)

        request_type = deploy_config_file['scheduling_info']['request_type']
        
        dep_obj = Deployer(deploy_config_file)
        if request_type == "start":
            tid = threading.Thread(target=dep_obj.create_files)
        elif request_type == "stop":
            tid = threading.Thread(target=dep_obj.send_stop_request)
        else:
            exit(0)
        tid.start()
        

    else:
        consumer = KafkaConsumer(
            "scheduler_to_deployer",
            bootstrap_servers=kafka_address,
            auto_offset_reset='earliest',
            group_id='consumer-group-a')
        print('starting the consumer')
        for msg in consumer:
            print("Reg user = {}".format(json.loads(msg.value)))
            deploy_config_file = json.loads(msg.value)
            request_type = deploy_config_file['scheduling_info']['request_type']
            dep_obj = Deployer(deploy_config_file)
            if request_type == "start":
                tid=threading.Thread(target=dep_obj.create_files)
            elif request_type == "stop":
                tid = threading.Thread(target=dep_obj.send_stop_request)
            else:
                continue
            tid.start()
    # threading.Thread(target=)

# ###KAFKA###
# f = open("deployConfig.json","r")
# ##########
# json_data = f.read()
# parsed = json.loads(json_data)
# f.close()
# req_type = parsed["scheduling_info"]["request_type"]
# if(req_type=="stop"):
#     ##FORWARD THE REQUEST
#     exit(0)
# req_file = open("requirements.txt","w")

# dependencies = parsed["environment"]["dependencies"]

# for dependency in dependencies:
#     if(dependency[1]!=""):
#         req_file.write(dependency[0]+"=="+dependency[1]+"\n")
#     else:
#         req_file.write(dependency[0]+"\n")
# req_file.close()

# docker_file = open("dockerfile","w")

# lang = parsed["environment"]["lang"] ## NOT USED .
# script = parsed["script_name"]


# docker_file.write("FROM python:3\n") ## "FROM " + lang
# docker_file.write("COPY requirements.txt ./\n")
# docker_file.write("RUN pip install --upgrade pip\n")
# docker_file.write("pip install --no-cache-dir -r requirements.txt\n")
# docker_file.write("ADD "+script+"\n")
# docker_file.write("CMD [\"python\", \"-u\"," + "\"{}\"".format(script)+ "]")
# docker_file.close()

# # FROM python:3
# # COPY requirements.txt ./


# # RUN pip install --upgrade pip && \
# #  pip install --no-cache-dir -r requirements.txt
# # ADD actionmanager.py .
# # CMD ["python","-u","actionmanager.py"]


