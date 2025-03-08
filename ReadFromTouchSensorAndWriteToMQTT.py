import ConfigurationManager
import datetime
import random
import time
import serial
from multiprocessing import Process, Queue
from threading import Thread
import random
from paho.mqtt import client as mqtt_client
import time
import json
import datetime
import os
import ast

class GetDataAndSendToCloud():

    def __init__(self):
        self.MQTTQueue = Queue()
        cm = ConfigurationManager.configManager()
        self.config = cm.readConfig()
        self.GetData()

    def GetData(self)->None:
        cloudConnection = Cloud(self.MQTTQueue, self.config)
        process = Process(target=cloudConnection.saveToCloud, daemon=True)
        process.start()

        Sensor = ReadFromMCU(self.MQTTQueue, self.config) 
     
        while True:
            Sensor.ReadDataFromMCU()
            time.sleep(0.5)

        

        

    
class ReadFromMCU():
    def __init__(self, MQTTQueue, config):
        print("dataReader init")
        self.MQTTQueue = MQTTQueue
        self.config = config

    def ReadDataFromMCU(self):        
        # ser = serial.Serial(self.config["config"]["serialPort"], 9600)
       
            try:
                # received_data = ser.readline() 
                # data_str = received_data.rstrip().lstrip().decode()
                data_str="<1,2,3,4,5,6,7,8>"
                data=[]
                if data_str.startswith("<") and data_str.endswith(">"):
                        data_str=data_str.removeprefix("<")
                        data_str=data_str.removesuffix(">")
                        data_split=data_str.split(",")
                        for i in range(0,len(data_split)):
                            data.append(int(data_split[i].strip()))
                        print("data", data)  
                        self.MQTTQueue.put([datetime.datetime.now(), data])     
                                
                else:
                    print("incorrect transmission")
                    
            except:
                print("couldm't read from MCU")


class Cloud():
    def __init__(self, MQTTQueue, config):
        print("MQTT init")
        self.MQTTQueue = MQTTQueue
        self.config = config
        self.bufferLength=int(self.config["config"]["bufferLength"])


    def connect_mqtt(self):
        broker = self.config["config"]["broker"]
        try:
            port = int(self.config["config"]["port"])
        except:
            print("falling back to port 1883")
            port = 1883
        topic = self.config["config"]["topic"]

        # Generate a Client ID with the publish prefix.
        client_id = f'publish-{random.randint(0, 1000)}'
        # username = 'emqx'
        # password = 'public'
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)

        osName=os.name
        if (osName=="nt"):
            client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, client_id)
        else:
            client = mqtt_client.Client(client_id)
        # 
        # client.username_pw_set(username, password)
        client.on_connect = on_connect
        try:
            client.connect(broker, port)
            return True, client
        except:
            return False, None


    def buildPayload(self, buffer):
        data = {}
        data_info={}
        data_sensor={}
        data_info["publish_time"]=datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f")
        # data_info["sensor_id"]=self.config["sensor_id"]
        # data_info["sensor_type"]=self.config["sensor_type"]
        # data_info["sensor_information"]=self.config["sensor_information"]
        # data_info["data_layout"]="timestamp, " + self.config["data_layout"]
        # data_info["data_type"]="string, " + self.config["data_type"]
        data["info"]=data_info
        for measurements in buffer:                 
            data_sensor[measurements[0].strftime("%m_%d_%Y_%H_%M_%S_%f")]=measurements[1]
        data["sensor"]=data_sensor
        data_JSON=json.dumps(data)
        return data_JSON                

    def saveToCloud(self):        
        buffer=[]
        connected, self.client=self.connect_mqtt()
        if connected:
        # if True:
            print("connected to MQTT broker")
            while True:
                if not self.MQTTQueue.empty():
                    
                    timestamp, values = self.MQTTQueue.get()
                    buffer.append([timestamp, values])
                    if len(buffer)>=self.bufferLength:
                            data_JSON = self.buildPayload(buffer)
                            self.publishToMQTT(data_JSON)
                            buffer.clear()

                else:
                    time.sleep(0.01)
        else: 
            print("couldn't connect to MQTT broker, will try again")
            time.sleep(5)
            while not self.MQTTQueue.empty():
                 timestamp, values = self.MQTTQueue.get()
            self.saveToCloud()

    def publishToMQTT(self, data_JSON):
        # self.writeToDisk(data_JSON)
        if self.client.is_connected():            
            self.client.publish(self.config["config"]["topic"], data_JSON)
        else:
            self.client.reconnect()
            self.client.publish(self.config["config"]["topic"], data_JSON)
            

    def writeToDisk(self, data_JSON):
        print("MQTT to disk")
        identifier=datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S_%f")+".json"
        dataFile=os.path.join(self.path , identifier)
        with open(dataFile, "w") as save_file:
            try:
                # payload=json.dumps(data)
                save_file.write(data_JSON)
                print("writing to disk")
            except:
                print("couldn't write to file or convert to JSON")


def main():
    GDSC = GetDataAndSendToCloud()

if __name__ == '__main__':                       # Program entry point
    main()   