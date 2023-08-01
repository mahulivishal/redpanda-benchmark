import yaml
from kafka.admin import KafkaAdminClient, NewTopic

print("Initialising ADMIN Configs............")
with open("admin.yml", "r") as f:
    configs = yaml.load(f, Loader=yaml.FullLoader)
configs = configs["configs"]
print(configs)

topics = []
for topic_configs in configs["topics"]:
    topics.append(NewTopic(name=topic_configs["topic"], num_partitions=topic_configs["partitions"],
                           replication_factor=topic_configs["replication"]))
print("Creating Topics............")
admin_client = KafkaAdminClient(bootstrap_servers=list(str(configs["redpanda_brokers"]).split(",")))
admin_client.create_topics(new_topics=topics)
print(f'Topics Created: {configs["topics"]}')
