import json
import time
from functools import reduce

import yaml
from kafka import KafkaConsumer

consumer = None
configs = None


def get_redpanda_consumer():
    global consumer
    if not consumer:
        print(f'Initialising {configs["system"]} consumer.......')
        consumer = KafkaConsumer(bootstrap_servers=list(str(configs["brokers"]).split(",")),
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
    latency_dict, partition_distribution = {}, {}
    try:
        print(f'Consumer RUNNING.............')
        for message in consumer:
            value = json.loads(message.value.decode("utf-8"))
            start = eval(value["timestamp"])
            end = eval(str(time.time()).replace('.', '')[0:13])
            if end > start:
                latency_dict[str(value["id"])] = {"delay": end - start}
            else:
                print(f'{end}, {value}')
            if message.partition in partition_distribution.keys():
                count = partition_distribution[message.partition]
                partition_distribution[message.partition] = count + 1
            else:
                partition_distribution[message.partition] = 1
    except Exception as e:
        print(f"Error occurred while consuming messages: {e}")
    finally:
        consumer.close()
        print("REPORT==============================")
        print(f"No of records consumed: {len(latency_dict.keys())}")
        print(f"Avg latency per record in MS: {get_latency_report(latency_dict)}")
        print(f'Partition Distribution: {get_partition_distribution(partition_distribution, len(latency_dict.keys()))}')
        print("====================================")


def get_latency_report(latency_dict):
    delays = []
    for r_id in latency_dict.keys():
        latency = latency_dict[r_id]
        delays.append(latency["delay"])
    return int(reduce(lambda a, b: a + b, delays) / len(delays))


def get_partition_distribution(partition_distribution, count):
    for partitions in partition_distribution.keys():
        count_partition = partition_distribution[partitions]
        partition_distribution[partitions] = count_partition / count * 100
    return partition_distribution


if __name__ == '__main__':
    initialise_configs()
    get_redpanda_consumer()
    start_consumer()
