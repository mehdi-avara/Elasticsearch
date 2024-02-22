from elasticsearch import Elasticsearch
import pandas as pd


es = Elasticsearch('http://localhost:9200')

csv_file_path = 'people-500000.csv'
df = pd.read_csv(csv_file_path)

total_rows = len(df)

rows_per_part = total_rows // 2000

data_parts = [df.iloc[i:i + rows_per_part] for i in range(0, total_rows, rows_per_part)]

for i, part in enumerate(data_parts):
    print(f"part {i} {part}")
    index_name = 'people'

    records = part.to_dict(orient='records')

    for record in records:
        es.index(index=index_name, body=record)

es.indices.refresh(index=index_name)
