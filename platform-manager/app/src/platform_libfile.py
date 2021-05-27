import requests


url = "http://0.0.0.0:5001"


def getSensorData(id,index):
    res = requests.post(url,json={"id":id,"index":index})
    return res.json()["data"]

def setSensorData(id,index,val=None):
    res = requests.post(url,json={"id":id,"index":index,"val":val})
    return res.json()["data"]