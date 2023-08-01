import json

import yaml
from kafka import KafkaConsumer

consumer = None
configs = None


def get_redpanda_consumer():
    global consumer
    if not consumer:
        consumer = KafkaConsumer(bootstrap_servers=list(str(configs["redpanda_brokers"]).split(",")),
                                 group_id=configs["group_id"],
                                 auto_offset_reset=configs["auto_offset_reset"],
                                 enable_auto_commit=configs["enable_auto_commit"])


def initialise_configs():
    global configs
    if not configs:
        print("Initialising Configs............")
        with open("rp.yml", "r") as f:
            configs = yaml.load(f, Loader=yaml.FullLoader)
        configs = configs["configs"]
        print(configs)


def start_consumer():
    print(f'Consumer Subscribing to TOPIC: {configs["topic"]}')
    consumer.subscribe(configs["topic"])
    try:
        print(f'Consumer RUNNING.............')
        for message in consumer:
            topic_info = f'Topic: {configs["topic"]} | Partition: {message.partition}'
            message_info = f"Key: {message.key}, Data: {message.value}"
            print(f"{topic_info}, {message_info}")
    except Exception as e:
        print(f"Error occurred while consuming messages: {e}")


if __name__ == '__main__':
    initialise_configs()
    get_redpanda_consumer()
    start_consumer()
