import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer
import json

def send_to_kafka(records, bootstrap_servers, kafka_topic):
    producer_config = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(producer_config)

    for record in records:
        json_message = json.dumps(record)

        producer.produce(kafka_topic, value=json_message)

    producer.flush()

csv_file_path = 'organizations-2000000.csv'
chunk_size = 1000 
df_chunks = pd.read_csv(csv_file_path, chunksize=chunk_size)

bootstrap_servers = 'localhost:9092'

kafka_topic = 'organizations'

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = []

    for i, chunk in enumerate(df_chunks):
        records = chunk.to_dict(orient='records')

        future = executor.submit(send_to_kafka, records, bootstrap_servers, kafka_topic)
        futures.append(future)

    for future in futures:
        future.result()
