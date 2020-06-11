import time
from mongo import Mongo
from mqtt import MQTT
from signal import pause

mongo = Mongo()
mqtt = MQTT(mongo)

mongo.connect()
mqtt.run()
time.sleep(20)
'''try:
    pause()
except KeyboardInterrupt:
    pass
'''
mqtt.stop()
mongo.disconnect()