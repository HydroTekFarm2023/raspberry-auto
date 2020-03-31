import paho.mqtt.client as mqtt
import datetime
import time
import jwt
import RPi.GPIO as GPIO 

ssl_private_key_filepath = '/home/pi/rsa_private.pem'
ssl_algorithm = 'RS256' # Either RS256 or ES256
root_cert_filepath = '/home/pi/roots.pem'
project_id = 'project-hydro1'
gcp_location = 'us-central1'
registry_id = 'raspberrypi'
device_id = 'raspberry_pi_1'

cur_time = datetime.datetime.utcnow()

green_led = 17
red_led = 27

en_1 = 5
en_2 = 6

trig = 23
echo = 24

max_height = 11.2
min_height = 13.2


def create_jwt():
    token = {
        'iat': cur_time,
        'exp': cur_time + datetime.timedelta(minutes=60),
        'aud': project_id
        }
    
    with open(ssl_private_key_filepath, 'r') as file:
        private_key = file.read()
        
        
    return jwt.encode(token, private_key, ssl_algorithm)


CLIENT_ID = 'projects/{}/locations/{}/registries/{}/devices/{}'.format(project_id, gcp_location, registry_id, device_id)
mqtt_topic = '/devices/{}/events'.format(device_id)

client = mqtt.Client(client_id=CLIENT_ID)

client.username_pw_set(username='ignored', password=create_jwt())

# Call Back Funtions
def error_str(rc):
    return '{}: {}'.format(rc, mqtt.error_string(rc))

def on_connect(unusued_client, unused_userdata, unused_flags, rc):
    print('on_connect', error_str(rc))

def on_publish(unused_client, unused_userdata, unused_mid):
    print('on_publish')

client.on_connect = on_connect
client.on_publish = on_publish

client.tls_set(ca_certs=root_cert_filepath)
client.connect('mqtt.googleapis.com', 8883)
client.loop_start()



def start_sensor_program():
    fill_to_top()
    print('WATER NOW AT TOP')
    GPIO.output(green_led, True)
    time.sleep(5)
    GPIO.output(green_led, False)
    drain_to_bottom()
    print('WATER NOW AT BOTTOM')
    GPIO.output(red_led, True)
    time.sleep(5)
    GPIO.output(red_led, False)
    

def GPIO_Setup():
    GPIO.setmode(GPIO.BCM)

    GPIO.setup(trig, GPIO.OUT)
    GPIO.setup(echo, GPIO.IN)

    GPIO.setup(en_1, GPIO.OUT)
    GPIO.setup(en_2, GPIO.OUT)
    
    GPIO.setup(green_led, GPIO.OUT)
    GPIO.setup(red_led, GPIO.OUT)

def measure_height(repeat_number):
    total_height = 0
    
    for i in range(1, (repeat_number + 1)):
        GPIO.output(trig, False)
        time.sleep(0.01)

        GPIO.output(trig, True)
        time.sleep(0.00001)
        GPIO.output(trig, False)

        while GPIO.input(echo)==0:
            pulse_start = time.time()

        while GPIO.input(echo)==1:
            pulse_end = time.time()
            
        pulse_duration = pulse_end - pulse_start

        distance = pulse_duration * 17150

        total_height = total_height + distance
        
        time.sleep(0.1)
    
    average_height = total_height/repeat_number
    return average_height

def pump_in():
    GPIO.output(en_1, False)
    GPIO.output(en_2, True)
    
def pump_out():
    GPIO.output(en_1, True)
    GPIO.output(en_2, False)
    
def pump_stop():
    GPIO.output(en_1, False)
    GPIO.output(en_2, False)
    
def fill_to_top():
    distance_from_sensor = measure_height(10)
    
    print('PUMP FILLING UP')
    print('Initial Current Distance: {}'.format(distance_from_sensor))

    while(distance_from_sensor > max_height):
        distance_from_sensor = measure_height(3)
        print('Current Distance: {}'.format(distance_from_sensor))
        pump_in()
        time.sleep(0.2)
        
    pump_stop()
    time.sleep(2)
    distance_from_sensor = measure_height(10)
    if distance_from_sensor > max_height:
        print('Wrong Current Distance: {}'.format(distance_from_sensor))
        while(distance_from_sensor > max_height):
            distance_from_sensor = measure_height(3)
            print('Current Distance: {}'.format(distance_from_sensor))
            pump_in()
            time.sleep(0.2)
        
    pump_stop()

def drain_to_bottom():
    distance_from_sensor = measure_height(10)

    print('PUMP DRAINING')
    print('Initial Current Distance: {}'.format(distance_from_sensor))

    while(distance_from_sensor < min_height):
        distance_from_sensor = measure_height(3)
        print('Current Distance: {}'.format(distance_from_sensor))
        pump_out()
        time.sleep(0.2)
        
    pump_stop()
    time.sleep(2)
    
    distance_from_sensor = measure_height(10)
    if distance_from_sensor < min_height:
        print('Wrong Current Distance: {}'.format(distance_from_sensor))
        while(distance_from_sensor < min_height):
            distance_from_sensor = measure_height(3)
            print('Current Distance: {}'.format(distance_from_sensor))
            pump_out()
            time.sleep(0.2)
        
    pump_stop()

'''payload = '{{ "ts": {}, "dist": {} }}'.format(int(time.time()), distance)
    
client.publish(mqtt_topic, payload, qos=1)
    
print("{}\n".format(payload))'''

GPIO_Setup()
start_sensor_program()
GPIO.cleanup()
client.loop_stop()
print('DONE')




    


    
    