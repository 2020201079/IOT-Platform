import os
import sys
import json
import time
import copy
import signal
import schedule
import threading
from bson import json_util
from kafka import KafkaConsumer
from kafka import KafkaProducer
from pymongo import MongoClient

cluster = MongoClient("mongodb+srv://akshay:akshay123@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = cluster["scheduler_logs"]
collection = db["deploy_configs"]

kafka_address=os.environ['KAFKA_ADDRESS']
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers=[kafka_address],value_serializer=json_serializer)

def get_attr(data):
    start_time, end_time, interval, days, repeat, job_id = 'NOW', '', '', [], 'NO', 0

    if('start_time' in data):
        start_time = data['start_time']

    if('end_time' in data):
        end_time = data['end_time']
    
    if('interval' in data):
        interval = data['interval']

    if('days' in data):
        days = data['days']
    
    if('repeat' in data):
        repeat = data['repeat']
    
    if('job_id' in data):
        job_id = data['job_id']
    
    return start_time, end_time, interval, days, repeat, job_id

def schedule_on_a_day(day, time, data, is_onetime, instance_id):
    if(day.lower() == 'monday'):
        schedule.every().monday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    elif(day.lower() == 'tuesday'):
        schedule.every().tuesday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    elif(day.lower() == 'wednesday'):
        schedule.every().wednesday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    elif(day.lower() == 'thursday'):
        schedule.every().thursday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    elif(day.lower() == 'friday'):
        schedule.every().friday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    elif(day.lower() == 'saturday'):
        schedule.every().saturday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    elif(day.lower() == 'sunday'):
        schedule.every().sunday.at(time).do(send_to_deployer, data, is_onetime).tag(str(instance_id))
    
def start_schedule(start_time, end_time, interval, days, repeat, job_id, instance_id, data):
    print("repeat: {}".format(repeat))
    print("days: {}".format(days))
    print("instance_id: {}".format(instance_id))
    print("interval: {}".format(interval))
    if(repeat.lower() == 'yes'):
        if(days == []):
            #run everyday at the given start time
            schedule.every().day.at(start_time).do(send_to_deployer, data, False).tag(str(instance_id))
            #send stop command to deployer everyday at the given end time
            if(end_time != ''):
                stop_data = copy.deepcopy(data)
                stop_data['scheduling_info']['request_type'] = 'stop'
                schedule.every().day.at(end_time).do(send_to_deployer, stop_data, False).tag(str(instance_id))
        else:
            print("days given")
            #run on that day every week at the given start time 
            for day in days:
                schedule_on_a_day(day, start_time, data, False, instance_id)
                if(end_time != ''):
                    stop_data = copy.deepcopy(data)
                    stop_data['scheduling_info']['request_type'] = 'stop'
                    schedule_on_a_day(day, end_time, stop_data, False, instance_id)
    else:
        print("repeat no")
        # Run only once or every interval
        if(days == []):
            print("days empty")
            # start at start_time
            if(interval == ''):
                schedule.every().day.at(start_time).do(send_to_deployer, data, True).tag(str(instance_id))
            else:
                if(start_time == ''):
                    ## TODO -> change to hours
                    print("will schedule now: {}".format(interval))
                    schedule.every(int(interval)).seconds.do(send_to_deployer, data, False).tag(str(instance_id))
                else:
                	schedule.every(int(interval)).hours.at(start_time).do(send_to_deployer, data, False).tag(str(instance_id))
            if(end_time != ''):
                stop_data = copy.deepcopy(data)
                stop_data['scheduling_info']['request_type'] = 'stop'
                schedule.every().day.at(end_time).do(send_to_deployer, stop_data, True).tag(str(instance_id))
        else:
            #start on the given day at given start time
            for day in days:
                schedule_on_a_day(day, start_time, data, True, instance_id)
                if(end_time != ''):
                    stop_data = copy.deepcopy(data)
                    stop_data['scheduling_info']['request_type'] = 'stop'
                    schedule_on_a_day(day, end_time, stop_data, True, instance_id)

def handle_schedule_info(sched_info, instance_id, data):

    if 'request_type' not in sched_info:
        print('[ERROR] : Scheduling request type not found')
        return -1

    request_type = sched_info['request_type']
    start_time, end_time, interval, days, repeat, job_id = get_attr(sched_info) 

    if(request_type.lower() == 'start'):
        start_schedule(start_time, end_time, interval, days, repeat, job_id, instance_id, data)
        collection.update_one({'instance_id':instance_id}, {"$set": data}, upsert=True)
        
    elif(request_type.lower() == 'stop'):
        schedule.clear(str(instance_id))
        send_to_deployer(data, False)
    else:
        print('[ERROR] : Invalid scheduling request type found')
        return -1
    
def run_pending_jobs():

    print('[CHECKING] Logs')
    existing_configs = collection.find({}, {'_id': False})
    
    cnt = 0
    for data in existing_configs:
        data = json.loads(json_util.dumps(data))
        #threading.Thread(target=handle_schedule_info, args=(data['scheduling_info'], data['instance_id'], data)).start()
        cnt += 1
    
    if(cnt > 0):
        print('[SCHEDULING]', cnt ,'pending jobs from logs')
    else:
        print('[CHECKED] Found no pending jobs in logs')
        
    while True:
        schedule.run_pending()
        time.sleep(1)

def send_to_deployer(data, is_onetime):
    print('[SENT] :', data['scheduling_info']['request_type'], ' request to Deployer')
    
    if(data['scheduling_info']['request_type'].lower() == 'stop'):
        collection.delete_one({'instance_id':data['instance_id']}) 
        
    producer.send('scheduler_to_deployer', data)

    if(is_onetime):
        collection.delete_one({'instance_id':data['instance_id']}) 
        return schedule.CancelJob

def consume_from_sensor_binder():

    consumer_for_sensor_binder = KafkaConsumer("sensor_binder_to_scheduler",
        bootstrap_servers=kafka_address,
        auto_offset_reset='earliest',
        group_id='consumer-group-a')

    for msg in consumer_for_sensor_binder:
        data = json.loads(msg.value)
        print('[RECEIVED] Deploy Config')
        threading.Thread(target=handle_schedule_info, args=(data['scheduling_info'], data['instance_id'], data)).start()

def signal_handler(sig, frame):
    print('[EXITING] Scheduler')
    os._exit(1)

if __name__ == "__main__":

    print('[STARTED] Scheduler')

    threading.Thread(target=run_pending_jobs, args=()).start()
    
    threading.Thread(target=consume_from_sensor_binder, args = ()).start()

    signal.signal(signal.SIGINT, signal_handler)

