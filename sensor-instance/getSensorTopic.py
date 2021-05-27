from bson.objectid import ObjectId
from kafka import KafkaConsumer
from pymongo import MongoClient
import flask
from flask import request, jsonify

app = flask.Flask(__name__)
#app.config["DEBUG"] = True


@app.route('/getSensorTopic', methods=["POST"])
def getSensorTopic():
    #content  = request.get_json(force=True)
    content = flask.request.json
    instance_id=content["id"]
    index=content["index"]
    return {"data": gettopic(instance_id,index)}


def gettopic(instance_id,index):
    #instance_id
    #index=0
    #o=ObjectId("6068d0bac3adf59832fa20b7")
    o = ObjectId(instance_id)
    #print (o)
    cluster = MongoClient(
        "mongodb+srv://shweta_10:shweta10@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

    #fetch sensor id from sensor_map
    db = cluster["binding_db"]
    collection = db["sensor_map"]
    #db.sensor_instance.find({"location":"Indore"} )
    myCursor = db.sensor_map.find({"_id":o} );

    for car in myCursor:
        sensor_id=car[str(index)]
        print(sensor_id)


    #fetch topic from sensor instances
    db = cluster["sensor_registory"]
    collection = db["sensor_instance"]
    myCursor1 = db.sensor_instance.find({"_id": sensor_id});
    for car in myCursor1:
        topic_name=car["topic"]
        print(topic_name)

    #read data from kafka topic and return
    # consumer = KafkaConsumer(
    #     "air_topic",
    #     bootstrap_servers='localhost:9092',
    #     #auto_offset_reset='earliest',
    #     group_id='consumer-group-s')
    # print('starting the consumer')
    # for msg in consumer:
    #     # print("Reg user = {}".format(json.loads(msg.value)))
    #     data = msg.value
    #     print(data)
        return (topic_name)




app.run(host="127.0.0.1",port="7500",debug=False)
