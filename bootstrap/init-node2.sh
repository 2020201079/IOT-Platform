#!/bin/bash

kafka_address="52.146.2.26:9092"
node2_address=IAS-Node-2@52.146.1.189

#platform manager
ssh node2_address sudo docker build -t plat-man -f /home/IAS-Node-2/iot_platform/platformManager_docker/dockerfile /home/IAS-Node-2/iot_platform/platformManager_docker/

ssh IAS-Node-2@52.146.1.189 sudo docker run -d --name platform-manager -p 5001:5001 -e KAFKA_ADDRESS=$kafka_address plat-man &

#sensor type registration 
ssh IAS-Node-2@52.146.1.189 sudo docker build -t sensor-type -f /home/IAS-Node-2/iot_platform/Sensor_Management_type_docker/dockerfile /home/IAS-Node-2/iot_platform/Sensor_Management_type_docker/

ssh IAS-Node-2@52.146.1.189 sudo docker run -d --name sensor-type -e KAFKA_ADDRESS=$kafka_address sensor-type &

#sensor instance registration
ssh IAS-Node-2@52.146.1.189 sudo docker build -t sensor-instance -f /home/IAS-Node-2/iot_platform/Sensor_Management_instance_docker/dockerfile /home/IAS-Node-2/iot_platform/Sensor_Management_instance_docker/

ssh IAS-Node-2@52.146.1.189 sudo docker run -d --name sensor-instance -e KAFKA_ADDRESS=$kafka_address sensor-instance &


#scheduler
ssh IAS-Node-2@52.146.1.189 sudo docker build -t scheduler -f /home/IAS-Node-2/iot_platform/scheduler_docker/dockerfile /home/IAS-Node-2/iot_platform/scheduler_docker/

ssh IAS-Node-2@52.146.1.189 sudo docker run -d --name scheduler -e KAFKA_ADDRESS=$kafka_address scheduler &

#deployer
ssh IAS-Node-2@52.146.1.189 sudo docker build -t deployer -f /home/IAS-Node-2/iot_platform/deployment_docker/dockerfile /home/IAS-Node-2/iot_platform/deployment_docker/

ssh IAS-Node-2@52.146.1.189 sudo docker run -d --name deployer -e KAFKA_ADDRESS=$kafka_address deployer &

#sensor binder
ssh IAS-Node-2@52.146.1.189 sudo docker build -t sensor-binder -f /home/IAS-Node-2/iot_platform/sensor_binder_docker/dockerfile /home/IAS-Node-2/iot_platform/sensor_binder_docker/

ssh IAS-Node-2@52.146.1.189 sudo docker run -d --name sensor-binder -e KAFKA_ADDRESS=$kafka_address sensor-binder &
