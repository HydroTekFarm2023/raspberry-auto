from typing import List
from datetime import datetime
import paho.mqtt.client as mqtt
import pymongo
import pymongo.database
import pymongo.collection
import pymongo.errors
import threading
import os
import time
from signal import pause


MONGO_URI = "mongodb://0.0.0.0:27017"  # mongodb://user:pass@ip:port || mongodb://ip:port
MONGO_DB = "buck"
MONGO_COLLECTION = "col1"
#MONGO_TIMEOUT = 20  # Time in seconds
MONGO_DATETIME_FORMAT = "%d/%m/%Y %H:%M:%S"

MONGO_URI = os.getenv("MONGO_URI", MONGO_URI)
MONGO_DB = os.getenv("MONGO_DB", MONGO_DB)
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", MONGO_COLLECTION)
#MONGO_TIMEOUT = float(os.getenv("MONGO_TIMEOUT", MONGO_TIMEOUT))
MONGO_DATETIME_FORMAT = os.getenv("MONGO_DATETIME_FORMAT", MONGO_DATETIME_FORMAT)


class Mongo(object):
    def __init__(self):
        self.client: pymongo.MongoClient = None
        self.database: pymongo.database.Database = None
        self.collection: pymongo.collection.Collection = None
        self.queue: List[mqtt.MQTTMessage] = list()

    def connect(self):
        
        self.client = pymongo.MongoClient(MONGO_URI)
        self.database = self.client.get_database(MONGO_DB)
        self.collection = self.database.get_collection(MONGO_COLLECTION)
        #print(self.collection)
        #print("Connecting Mongo")
        
    def disconnect(self):
        #print("Disconnecting Mongo")
        if self.client:
            self.client.close()
            self.client = None

    def connected(self) -> bool:
        if not self.client:
            return False
            try:
                self.client.admin.command("ismaster")
            except pymongo.errors.PyMongoError:
                return False
        else:
            return True

    def _enqueue(self, msg: mqtt.MQTTMessage):
        #print("Enqueuing")
        self.queue.append(msg)
        # TODO process queue

    def __store_thread_f(self, msg: mqtt.MQTTMessage):
        seg=msg.topic.split('/')
        
        try:
        
            D2=eval(msg.payload.decode())
            
            result = self.collection.update_one({
                "grow_room_id": seg[0],
                "h_system_id":seg[2],
                "nsamples":{'$lt':3}
                },
                {
                    '$push': { 
                        "samples": D2
                        
                         },
                    '$inc': { "nsamples": 1 }
                    
                    }, upsert=True)
                #"topic": msg.topic,
                
                # "retained": msg.retain,
                #"qos": msg.qos,
                #"timestamp": int(now.timestamp()),
                #"datetime":
            #print("Stored")
            if not result.acknowledged:
                # Enqueue message if it was not saved properly
                self._enqueue(msg)
        except Exception as ex:
            print(ex)

    def _store(self, msg):
        #print("storing")
        th = threading.Thread(target=self.__store_thread_f, args=(msg,))
        th.daemon = True
        th.start()

    def save(self, msg: mqtt.MQTTMessage):
        #print("Saving")
        if msg.retain:
            print("Skipping retained message")
            return
        if self.connected():
            self._store(msg)
        else:
            self._enqueue(msg)
