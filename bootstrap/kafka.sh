#!/bin/bash

fuser -k 9092/tcp # it will kill if any previous zookeeper was running
fuser -k 2181/tcp

cd /opt/

if [[ ! -d "/opt/kafka_2.13-2.7.0" ]]
then
    wget https://downloads.apache.org/kafka/2.7.0/kafka_2.13-2.7.0.tgz

    tar -xvzf kafka_2.13-2.7.0.tgz
fi

cd kafka_2.13-2.7.0

bin/zookeeper-server-start.sh config/zookeeper.properties &

JMX_PORT=8004 bin/kafka-server-start.sh config/server.properties &
