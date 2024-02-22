from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch
import json

es = Elasticsearch('http://localhost:9200')



bootstrap_servers = 'localhost:9092'

kafka_topic = 'organizations'

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'your_consumer_group_id',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
}


consumer = Consumer(consumer_config)
consumer.subscribe([kafka_topic])

def index_to_elasticsearch(record, index_name):
    es.index(index=index_name, body=record)

index_name = 'organizationsdata5'

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        try:
            record = json.loads(msg.value().decode('utf-8'))
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            continue
        print(record)
        index_to_elasticsearch(record, index_name)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
