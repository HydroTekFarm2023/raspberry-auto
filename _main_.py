import time
from mongo import Mongo
from mqtt import MQTT

print("Start Hydro Mqtt Client")

mongo = Mongo()
mqtt = MQTT(mongo)

mongo.connect()
isSuccess = False
ex = None
failCounter = 0
maxFail = 10
while not isSuccess:
    try: 
        mqtt.run()
        isSuccess = True
    except Exception as exception: 
        ex = exception
        print(ex)
        isSuccess = False
        failCounter += 1
    
    if failCounter >= maxFail:
        break
    
    time.sleep(30)     
    print(failCounter)
    
if isSuccess:
    while True:
        time.sleep(60)

print("Unable to Connect to Broker: Ending Program")

