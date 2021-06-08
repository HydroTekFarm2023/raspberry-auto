import time
from mongo import Mongo
from mqtt import MQTT

mongo = Mongo()
mqtt = MQTT(mongo)

mongo.connect()
mqtt.run()

while True:
    time.sleep(60)

