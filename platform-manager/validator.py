import os
import json
import shutil
import zipfile
import mongoDBConnector

def validate_appConfig(filepath):

    f = open (filepath, "r")
    data = json.loads(f.read())

    if ('application_name' not in data.keys()):
        print('[ERROR] : application_name not found in appConfig')
        return -1

    if ('application_id' not in data.keys()):
        print('[ERROR] : application_id not found in appConfig')
        return -1

    if ('developer_id' not in data.keys()):
        print('[ERROR] : developer_id not found in appConfig')
        return -1
    
    if ('environment' not in data.keys()):
        print('[ERROR] : environment not found in appConfig')
        return -1
    
    if ('algorithm_list' not in data.keys()):
        print('[ERROR] : algorithm_list not found in appConfig')
        return -1
    
    algos = data['algorithm_list']
    algo_files = []
    sensor_types = []

    for algo in algos:
        algo_files.append(algo['script']['name'])
        
        for sensors in algo['input_sensors']:
            sensor_types.append(sensors["sensor_type"])

    sensor_types = set(sensor_types)

    for file in algo_files:
        if(os.path.isfile('./app/src/'+file) == False):
            print('[ERROR] : ' + file +' not found')
            return -1
    
    sensorsInDB = mongoDBConnector.getSensorTypeList()
    for sensorType in sensor_types:
        if sensorType not in sensorsInDB:
            print("Sensor type is missing")
            return -1
    return 1
        
    # TODO -> check if sensor types are present in DB

def validate_deployConfig(filepath):
    f = open (filepath, "r")
    data = json.loads(f.read())

    if ('application_id' not in data.keys()):
        print('[ERROR] : application_id not found in deployConfig')
        return -1
    
    if ('deployables' not in data.keys()):
        print('[ERROR] : deployables not found in deployConfig')
        return -1
    
    for deployable in data['deployables']:
        if ('algorithm_name' not in deployable.keys()):
            print('[ERROR] : algorithm_name not found in a deployConfig deployable')
            return -1
        
        if ('sensor_info' not in deployable.keys()):
            print('[ERROR] : sensor_info not found in a deployConfig deployable')
            return -1

def validate_appzip(filepath_to_zip):
    # Removes existing ./src folder
    if(os.path.isdir('./src')):
        shutil.rmtree('./src')

    #add try catch later
    with zipfile.ZipFile(filepath_to_zip, 'r') as zip_ref:
        zip_ref.extractall('./')

    if(os.path.isdir('./app/src') == False):
        print('[ERROR] : src folder not found')
        return -1
    
    if(os.path.isfile('./app/appConfig.json') == False):
        print('[ERROR] : appConfig.json not found')
        return -1
    
    return validate_appConfig('./app/appConfig.json')


def validate_sensor_type(filepath):
    with open(filepath,"r") as f:
        data = None
        try:
            data = json.load(f)
    
            if(len(data.keys()) != 1):
                print("Invalid Sensor Type File")
                return -1
            for sensors in data[list(data.keys())[0]]:
                if(len(sensors.keys()) != 4):
                    print("Invalid sensor type file")
                    return -1
                K = ["sensor_type_name","company","sensor_data_structure","control_functions"]
                for key in K:
                    sensors[key]
                    if(key == "control_functions"):                
                        l = sensors[key]["number_of_functions"]
                        funcs = sensors[key]["function_details"]
                        if(len(funcs) != l):
                            print("Invalid function numbers")
                            return -1
                        for func in funcs:
                            paramlen = func["number_of_parameters"]
                            params = func["params"]
                            if(len(params) != paramlen):
                                print("Invalid params length")
                                return -1

        except json.JSONDecodeError:
            print("json not proper")
            return -1
        except KeyError:
            print("key not present")
            return -1
        else:
            print("Validated..")
            return 1

def validate_sensor_instance(filepath):
    with open(filepath,"r") as f:
        data = None
        try:
            data = json.load(f)
    
            if(len(data.keys()) != 1):
                print("Invalid Sensor Type File")
                return -1
            
            for sensors in data[list(data.keys())[0]]:
                #print(sensors)
                K = ["sensor_type","ip","port","no_of_fields"]                
                for key in K:
                    sensors[key]
                    if(key == "no_of_fields"):
                        if(len(sensors.keys()) - 4 != sensors[key]):
                            print("invalid params")
                            return -1

        except json.JSONDecodeError:
            print("json not proper")
            return -1
        except KeyError:
            print("key not present")
            return -1
        else:
            print("Validated..")
            return 1


if __name__ == "__main__":

    #validate_appzip('./app.zip')
    #validate_appConfig('./src/appConfig.json')
    #validate_deployConfig('./deployConfig.json')
    #validate_sensor_type('./sensorTypeRegistration.json')
    validate_sensor_instance('./sensorInstance.json')