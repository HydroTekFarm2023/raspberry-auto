from typing import List
import datetime
import paho.mqtt.client as mqtt
import pymongo
import pymongo.database
import pymongo.collection
import pymongo.errors
import threading
import os

DEFAULT_MONGO_COLLECTION = "sensor_data"

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PWD = os.getenv("MONGO_PWD")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", DEFAULT_MONGO_COLLECTION)

print(MONGO_HOST, MONGO_USERNAME, MONGO_PWD, MONGO_DB_NAME, MONGO_COLLECTION)

MONGO_URI = "mongodb+srv://" + MONGO_USERNAME + ":" + MONGO_PWD + "@cluster0.x5wba.gcp.mongodb.net/" + MONGO_DB_NAME + "?retryWrites=true&w=majority"

class Mongo(object):
    def __init__(self):
        self.client: pymongo.MongoClient = None
        self.database: pymongo.database.Database = None
        self.sensorDataCollection: pymongo.collection.Collection = None
        self.queue: List[mqtt.MQTTMessage] = list()

    def connect(self):
        print('connecting to database')
        self.client = pymongo.MongoClient(MONGO_URI)
        self.database = self.client.get_database(MONGO_DB_NAME)
        self.sensorDataCollection = self.database.get_collection(MONGO_COLLECTION)
        
        
    def disconnect(self):
        if self.client:
            self.client.close()
            self.client = None

    def connected(self) -> bool:
        if self.client:
            return True
        else:
            print("Not Connected To Mongo")
            return False

    def _enqueue(self, msg: mqtt.MQTTMessage):
        self.queue.append(msg)

    def __store_thread_f(self, msg: mqtt.MQTTMessage):
        topicParams = msg.topic.split('/')
        sample = eval(msg.payload.decode())

        # Incoming Time Stamp Format: 2020-07-07T19-01-59Z
        # Format Time Stamp to remove Letters and Commas
        tempTimeStamp = sample["time"].rstrip().replace('-',' ').replace(':', ' ').replace('T', ' ').replace('Z', '').replace(' ',',')
        sample["time"] = datetime.datetime.strptime(tempTimeStamp,'%Y,%m,%d,%H,%M,%S')

        try:
            if len(topicParams) == 2:
                result = self.sensorDataCollection.update_one(
                    {
                        "topicID": topicParams[1],
                        "nsamples":{'$lt':5}
                    },
                    {
                        '$push': { 
                            "samples": sample                        
                        },
                        '$set': { "last_time": sample["time"] },
                        '$setOnInsert': { "first_time" : sample["time"] },
                        '$inc': { "nsamples": 1 }
                    }, 
                    upsert = True)

                # Enqueue message if it was not saved properly
                if not result.acknowledged:
                    self._enqueue(msg)

            else: print("Incorrect topic name. Use topic with 2 or 3 fields.")

        except Exception as ex:
                print(ex)

    def _store(self, msg):
        th = threading.Thread(target=self.__store_thread_f, args=(msg,))
        th.daemon = True
        th.start()

    def save(self, msg: mqtt.MQTTMessage):
        if msg.retain:
            print("Skipping retained message")
            return
        if self.connected():
            self._store(msg)
        else:
            print("Queing Message")
            self._enqueue(msg)

