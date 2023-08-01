import json
import random
import string
import time
import yaml

from kafka import KafkaProducer

producer = None
configs = None


def get_redpanda_producer():
    global producer
    if not producer:
        print("Initialising REDPANDA producer.......")
        producer = KafkaProducer(bootstrap_servers=list(str(configs["redpanda_brokers"]).split(",")),
                                 api_version=(1, 0, 0),
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def initialise_configs():
    global configs
    if not configs:
        print("Initialising Configs............")
        with open("rp.yml", "r") as f:
            configs = yaml.load(f, Loader=yaml.FullLoader)
        configs = configs["configs"]
        print(configs)


def on_success(metadata):
    print(f"Body '{metadata.body}' | offset {metadata.offset}")


def on_error(e):
    print(f"Error sending message: {e}")


def mock_data():
    vin = f'{configs["vin_prefix"]}-{str(random.randrange(configs["no_of_vins"]))}'
    states = ["UNLOCK", "LOCK", "CHARGING", "PARKED"]
    event_types = ["BCM_EVENT", "BMS_EVENT", "FAULT", "HMI_EVENT", "GPS_EVENT"]
    sw_versions = ["v1.0", "v2.0", "v3.0", "v4.0", "v5.0"]
    v_id = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(6))
    payload = {"timestamp": str(time.time()).replace('.', '')[0:13],
               "schema_version": "1.0",
               "packet_id": random.randrange(configs["no_of_records"]),
               "v_id": v_id,
               "vin": vin,
               "scooter_state": random.choice(states),
               "event_type": random.choice(event_types),
               "event_data": [{"code": "code", "value": 1.0}],
               "raw_can_data": {"data": {"code": "data", "value": 0}},
               "sw": random.choice(sw_versions)
               }
    return payload


# Produce asynchronously with callbacks
def push_to_redpanda():
    global producer
    print("Pushing records to redpanda.......")
    for i in range(1, configs["no_of_records"]):
        payload = mock_data()
        producer.send(configs["topic"], value=payload)
    flush_producer()


def flush_producer():
    global producer
    producer.flush()
    producer.close()


if __name__ == '__main__':
    initialise_configs()
    get_redpanda_producer()
    push_to_redpanda()
