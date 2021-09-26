import os
import time
import paho.mqtt.client as mqtt
import urllib.request
import shutil
import threading
import json

ota_download_start = 0

base_path = '/var/www/html/'
ota_directory = 'OTA'
ota_bin_filename = 'mqtt_ota.bin'

mac_address_table = []
mac_file = "/home/pi/mac.txt"
http_header = "http://"
server_ip = "192.168.0.103"

SUBSCRIBE_FROM_IONIC_APP_TOPIC  = "fertigation_ota_notification"
PUBLISH_TO_ESP_TOPIC            = "fertigation_ota_push"
SUBSCRIBE_FROM_ESP_TOPIC        = "fertigation_ota_result"

def create_folder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
            os.chmod(directory, 0o777)
            print("Directory '% s' created" % directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)

def download_image(url, fw_download_path):
    # Download the file from `url` and save it locally under `file_name`:
    with urllib.request.urlopen(url) as response, open(fw_download_path, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)

def on_disconnect(client, userdata, rc):
    print("ESP OTA ack timeout...Disconnecting from MQTT Broker and creating MAC address file: ", mac_file)
    with open(mac_file, 'w') as f:
        for item in mac_address_table:
            f.write("%s\n" % item)
    print("Script END")

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc!=0:
        print("Exit: Bad Connection - result code "+str(rc))
        exit()

    print("Connected to MQTT Broker... Subscribing for OTA start command")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(SUBSCRIBE_FROM_IONIC_APP_TOPIC)
    client.subscribe(SUBSCRIBE_FROM_ESP_TOPIC)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    #print(msg.topic+" "+str(msg.payload))
    global ota_download_start
    if msg.topic == SUBSCRIBE_FROM_IONIC_APP_TOPIC:
        if ota_download_start == 1:
            print("OTA already in progress")
        else:
            print("Got msg to download the image from Server")
            ota_download_start = 1
            print(msg.payload)
            app_response = msg.payload.decode('utf-8')
            print(app_response)
            json_response = json.loads(app_response)
            device_id = json_response['device_id'] 
            #print(device_id)
            version = json_response['version'] 
            print(version)
            endpoint = json_response['endpoint'] 
            print(endpoint)
            fw_download_path = base_path + ota_directory 
            create_folder(fw_download_path)
            fw_download_path = base_path + ota_directory + '/' + version 
            create_folder(fw_download_path)
            fw_download_path = base_path + ota_directory + '/' + version + '/' + ota_bin_filename
            print(fw_download_path)
            download_image(endpoint, fw_download_path)
            print("Image downloaded from Server...Publishing OTA bin path to ESP")
            
            data = {}
            data['device_id'] = device_id
            data['version'] = version
            endpoint = http_header + server_ip + '/' + ota_directory + '/' + version + '/' + ota_bin_filename 
            data['endpoint'] = endpoint
            publish_cmd = json.dumps(data)
            print(publish_cmd)
            
            client.publish(PUBLISH_TO_ESP_TOPIC, publish_cmd)

    if msg.topic == SUBSCRIBE_FROM_ESP_TOPIC:
        mac_address_table.append(msg.payload.decode('utf-8'))

print("Script START")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

client.connect("localhost", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
client.loop_forever()

