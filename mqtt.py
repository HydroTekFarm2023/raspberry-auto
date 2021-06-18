from mongo import Mongo
import paho.mqtt.client as mqtt
import os

MQTT_CLIENT_NAME = "pythonClient"
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
MQTT_QOS = 1

MQTT_CLIENT_NAME = os.getenv("MQTT_CLIENT_NAME", MQTT_CLIENT_NAME)
MQTT_BROKER = os.getenv("MQTT_BROKER", MQTT_BROKER)
MQTT_PORT = os.getenv("MQTT_PORT", MQTT_PORT)
MQTT_KEEPALIVE = os.getenv("MQTT_KEEPALIVE", MQTT_KEEPALIVE)
MQTT_QOS = os.getenv("MQTT_QOS", MQTT_QOS)

MQTT_TOPICS = ("live_data/#")  # Array of topics to subscribe; '#' subscribe to ALL available topics

class MQTT(object):
    def __init__(self, mongo: Mongo):
        self.mongo: Mongo = mongo
        self.mqtt_client = mqtt.Client(MQTT_CLIENT_NAME)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    # noinspection PyUnusedLocal
    @staticmethod
    def on_connect(client: mqtt.Client, userdata, flags, rc):
        print('Connected to Broker: ' + MQTT_BROKER + ":" + str(MQTT_PORT))
        for topic in MQTT_TOPICS:
            client.subscribe(topic, MQTT_QOS)

    # noinspection PyUnusedLocal
    def on_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        self.mongo.save(msg)

    def run(self):
        print('Conencting to Broker at:', MQTT_BROKER + ":" + str(MQTT_PORT))
        self.mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
        self.mqtt_client.loop_start()

    def stop(self):
        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()
