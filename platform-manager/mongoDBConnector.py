import pymongo
import json
import bson
import base64
from bson.binary import Binary
from pymongo import MongoClient

'''
cluster = MongoClient("mongodb+srv://akshay:akshay123@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")

db = cluster["AppRepo"]
collection = db["ID_sourceFolder"]

file_used="/home/varun/IAS/hackathon2/UI/templates.zip"
with open(file_used,"rb") as f:
    encoded = Binary(f.read())

collection.insert_one({"Application id ": 1,"folder":encoded,"description":"testing"})


#code to download zip folder from mongo
'''
'''
x = collection.find({},{'folder': 1})

for data in x:
    print(data)
    f = open("mongo.zip","wb")
    f.write(data['folder'])
    f.close()
'''

def getSensorTypeList():
    cluster = MongoClient("mongodb+srv://akshay:akshay123@cluster0.bh25q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    db = cluster["sensor_registory"]
    collection = db["sensor_type"]
    x = collection.find({},{'sensor_type_name': 1})
    ans=[]
    for data in x:
        print(data['sensor_type_name'])
        ans.append(data['sensor_type_name'])
    return ans

if __name__ == '__main__':
    getSensorTypeList()