import ConfigurationManager
import random
from paho.mqtt import client as mqtt_client
import os


class ReadFromMQTT():
    def __init__(self, config):
        print("dataReader init")
        self.config = config

        self.broker = self.config["config"]["broker"]
        try:
            self.port = int(self.config["config"]["port"])
        except:
            print("falling back to port 1883")
            self.port = 1883
        self.topic = self.config["config"]["topic"]
        # broker = 'broker.emqx.io'
        # port = 1883
        # topic = "python/mqtt"
        # Generate a Client ID with the subscribe prefix.
        self.client_id = f'subscribe-{random.randint(0, 100)}'
        # username = 'emqx'
        # password = 'public'


    def connect_mqtt(self) -> mqtt_client:
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)
        osName=os.name
        if (osName=="nt"):
            client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1, self.client_id)
        else:
            client = mqtt_client.Client(self.client_id)
        # client.username_pw_set(username, password)
        client.on_connect = on_connect
        client.connect(self.broker, self.port)
        return client


    def subscribe(self, client: mqtt_client):
        def on_message(client, userdata, msg):
            print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

        client.subscribe(self.topic)
        client.on_message = on_message


def run():
    cm = ConfigurationManager.configManager()
    config = cm.readConfig()
    Cloud = ReadFromMQTT(config)
    client = Cloud.connect_mqtt()
    Cloud.subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()